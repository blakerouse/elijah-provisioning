
import json
import os
import random
import tempfile
import threading
import time
import urllib
import urllib2
from collections import defaultdict
from subprocess import Popen, CalledProcessError

import yaml
from flask import Flask, abort, jsonify, make_response, request, Response
from elijah.gateway.lease_parser import Leases
from elijah.provisioning.synthesis_client import Client, Protocol

app = Flask(__name__)

network_lock = threading.Lock()


class OutOfNetworksError(Exception):
    """Raised when no more networks are available."""


class MissingConfigError(Exception):
    """Raised when the configuration is wrong."""


def random_mac():
    """Generate a random MAC address."""
    return "52:54:00:%02x:%02x:%02x" % (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
        )


def synchronized(lock):
    """
    Decorator that prevents multiple threads to use the same function
    at the same time.
    """
    def wrap(f):
        def newFunction(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return newFunction
    return wrap


def get_app_state():
    """Return the state of the application."""
    return app.config['CLOUDLET_STATE']


def get_all_networks():
    """Return all the networks available.

    This uses the networks defined in the config and the network must also
    exist on at least one cloudlet.
    """
    found_networks = set()
    for cloudlet, data in app.config['CLOUDLET_CONFIG']['cloudlets'].items():
        for network, interface in data['networks'].items():
            found_networks.add(network)
    return {
        network: interface
        for network, interface in (
            app.config['CLOUDLET_CONFIG']['networks'].items())
        if network in found_networks
    }


@synchronized(network_lock)
def atomic_network_allocate(user_id, migration=None):
    """Reserve a free network for `user_id`."""
    used_networks = [
        network['network']
        for _, network in get_app_state().items()
    ]
    available_networks = [
        network
        for network, interface in get_all_networks().items()
        if network not in used_networks
    ]
    if not available_networks:
        app.logger.error(
            "no available networks for new user '%s'", user_id)
        raise OutOfNetworksError()
    selected_network = available_networks[0]
    app.logger.info(
        "selected new network '%s' for user '%s'", selected_network, user_id)
    data = {
        'network': selected_network,
        'apps': {},
    }
    if migration:
        data['migration'] = migration
    get_app_state()[user_id] = data
    return get_app_state()[user_id]


@synchronized(network_lock)
def atomic_network_release(user_id, app_id):
    """Release the network `user_id` and `app_id` is using."""
    network = get_user_network(user_id, create=False)
    if not network:
        app.logger.info(
            "no need to release network for user '%s', currently no "
            "network allocated", user_id)
        return
    network['apps'].pop(app_id, None)
    if not network['apps']:
        app.logger.info(
            "releasing network '%s' for user '%s', no apps running",
            network['network'], user_id)
        del get_app_state()[user_id]


def get_user_network(user_id, create=False, migration=None):
    """Return the tenant network for the user to use."""
    network = get_app_state().get(user_id)
    if network is None and create:
        return atomic_network_allocate(user_id, migration=migration)
    return network


def get_network_and_app(user_id, app_id):
    """Get the network and the app.

    Aborts with 404 if cannot be found.
    """
    network = get_user_network(user_id)
    if not network:
        abort(404)
    app = network['apps'].get(app_id)
    if not app:
        abort(404)
    return network, app


def select_cloudlet(network):
    """Select the best cloudlet to start a new VM on."""
    found_cloudlets = {}
    for cloudlet, data in app.config['CLOUDLET_CONFIG']['cloudlets'].items():
        for connected_network, interface in data['networks'].items():
            if connected_network == network:
                found_cloudlets[cloudlet] = data
                break
    # Pick the cloudlet that is running the lease number of apps.
    def _cloudlet_sorter(cloudlet):
        return sum(
            len(network['apps'])
            for _, network in get_app_state().items()
        )
    sorted_cloudlets = sorted(found_cloudlets.keys(), key=_cloudlet_sorter)
    if not sorted_cloudlets:
        return None
    data = found_cloudlets[sorted_cloudlets[0]]
    data['name'] = sorted_cloudlets[0]
    return data


@synchronized(network_lock)
def start_network(network):
    """Start the networking for the network."""
    started = network.get('open', False)
    if started:
        return False
    app.logger.info("starting network '%s'", network['network'])
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network['network']]
    external_ip = config.get('vpn', {}).get('external_ip')
    port_offset = config.get('vpn', {}).get('port_offset')
    env = os.environ.copy()
    if external_ip:
        env['OUTSIDE_IP'] = external_ip
    if port_offset:
        env['CLOUDLET_VPN_PORT_OFFSET'] = port_offset
    migration = network.get('migration')
    if migration:
        write_leases(network['network'], migration['leases'])
        env['TENANT_NETWORK_ID'] = migration['vid']
    process = Popen(
        ['cloudlet-add-vlan', net_info['interface'], str(net_info['vid'])],
        env=env)
    if process.wait() != 0:
        raise CalledProcessError(process.returncode, 'cloudlet-add-vlan')
    network['open'] = True
    return True


@synchronized(network_lock)
def stop_network(network):
    """Stop the networking for the network."""
    started = network.get('open', False)
    if not started:
        return
    app.logger.info("stopping network '%s'", network['network'])
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network['network']]
    port_offset = config.get('vpn', {}).get('port_offset')
    env = os.environ.copy()
    if port_offset:
        env['CLOUDLET_VPN_PORT_OFFSET'] = port_offset
    process = Popen(
        ['cloudlet-delete-vlan', str(net_info['vid'])],
        env=env)
    if process.wait() != 0:
        raise CalledProcessError(process.returncode, 'cloudlet-delete-vlan')
    del network['open']


def launch_vm(cloudlet, network, mac, overlay_path):
    """Launch the VM on the `cloudlet` connected to network."""
    cloudlet_ip = cloudlet['ip']
    cloudlet_port = cloudlet.get('port', 8021)
    cloudlet_interface = cloudlet['networks'][network]
    cloudlet_options = dict()
    cloudlet_options[Protocol.SYNTHESIS_OPTION_DISPLAY_VNC] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_EARLY_START] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_INTERFACE] = (
        cloudlet_interface)
    cloudlet_options[Protocol.SYNTHESIS_OPTION_INTERFACE_MAC] = mac
    cloudlet_client = Client(
        cloudlet_ip, cloudlet_port,
        overlay_file=overlay_path,
        app_function=lambda: None, synthesis_option=cloudlet_options)
    cloudlet_client.provisioning()
    return cloudlet_client


def wait_for_ip(network, mac):
    """Wait for the IP address for `mac` on `network`."""
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network]
    leases_path = os.path.join(
        '/var/lib/cloudlet/dnsmasq/vlan-%d.leases' % net_info['vid'])
    for _ in range(4 * 30):  # 30 seconds.
        leases = Leases(leases_path)
        for entry in leases.entries():
            if entry.mac.lower() == mac.lower():
                return entry.ip
        time.sleep(0.25)
    return None


def write_leases(network, leases):
    """Write the entire leases file."""
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network]
    os.makedirs('/var/lib/cloudlet/dnsmasq')
    leases_path = os.path.join(
        '/var/lib/cloudlet/dnsmasq/vlan-%d.leases' % net_info['vid'])
    with open(leases_path, 'w') as stream:
        stream.write(leases)


def dump_leases(network):
    """Dump the entire leases file."""
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network]
    leases_path = os.path.join(
        '/var/lib/cloudlet/dnsmasq/vlan-%d.leases' % net_info['vid'])
    with open(leases_path, 'r') as stream:
        return stream.read()


def get_vpn_client_config(network):
    """Get the VPN client configuration for `network`."""
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network]
    ovpn_path = os.path.join(
        '/var/lib/cloudlet/vpn/tenant%d-client.ovpn' % net_info['vid'])
    with open(ovpn_path, 'r') as stream:
        return stream.read()


def start_migration(user_app):
    """Start the migration of the user application."""
    def _run():
        client = user_app['client']
        tmpdir = tempfile.mkdtemp()
        residue_path = os.path.join(tmpdir, 'residue.zip')
        user_app['migrate_path'] = residue_path
        with open(residue_path, 'wb') as stream:
            client.handoff(stream)

    thread = threading.Thread(target=_run)
    thread.start()
    user_app['migrate_thread'] = thread


def wait_for_migration(migrate, user_id, app_id):
    """Wait for the migration to be complete so the residue can be pulled."""
    while True:
        migrate_response = urllib2.urlopen(
            migrate + "?user_id=%s&app_id=%s" % (user_id, app_id))
        if migrate_response.getcode() != 200:
            app.logger.error(
                "failed to request migration information from "
                "'%s' for app '%s' user '%s'",
                migrate, app_id, user_id)
            abort(400)
        migrate_response = json.loads(migrate_response.read())
        migrate_status = migrate_response.get('migrate', 'inprogress')
        if migrate_status == 'complete':
            break
        else:
            time.sleep(1)


@app.route('/', methods=['GET', 'POST', 'DELETE'])
def index():
    """Endpoint to get app information and to start a new application."""
    user_id = request.values.get("user_id")
    app_id = request.values.get("app_id")
    if request.method == 'POST':
        migrate = request.values.get("migrate")
        migrate_network_info = None
        migrate_app_info = None
        if migrate:
            migrate = urllib.unquote(migrate)
            if migrate.endswith('/'):
                migrate += 'migrate'
            else:
                migrate += '/migrate'
            app.logger.info(
                "requesting migration information from "
                "'%s' for app '%s' user '%s'",
                migrate, app_id, user_id)
            migrate_response = urllib2.urlopen(
                migrate + "?user_id=%s&app_id=%s" % (user_id, app_id))
            if migrate_response.getcode() != 200:
                app.logger.error(
                    "failed to request migration information from "
                    "'%s' for app '%s' user '%s'",
                    migrate, app_id, user_id)
                abort(400)
            migrate_response = json.loads(migrate_response.read())
            migrate_network_info = migrate_response.get('network', {})
            migrate_app_info = migrate_response.get('app', {})
            if ('vid' not in migrate_network_info or
                    'leases' not in migrate_network_info or
                    'ip' not in migrate_app_info or
                    'mac' not in migrate_app_info):
                app.logger.error(
                    "invalid migration information from "
                    "'%s' for app '%s' user '%s'",
                    migrate, app_id, user_id)
                abort(400)
        try:
            network = get_user_network(
                user_id, create=True, migration=migrate_network_info)
        except OutOfNetworksError:
            abort(400)
        user_app = network['apps'].get(app_id)
        if user_app:
            app.logger.error(
                "app '%s' for user '%s' is already running in cloudlet",
                app_id, user_id)
            abort(400)
        # Starting a new application on the selected network. Find the
        # best cloudlet server to run the application on.
        selected_cloudlet = select_cloudlet(network['network'])
        if not selected_cloudlet:
            app.logger.error(
                "failed to select a cloudlet server to run app '%s' "
                "for user '%s'", app_id, user_id)
            abort(400)
        # If this is a migration we need to wait until the migration is ready
        # to get the residue from the other gateway.
        if migrate is not None:
            wait_for_migration(migrate, user_id, app_id)
        # Determine if a file was provided on the request to create.
        overlay_path = os.path.join(
            app.config['OVERLAYS_PATH'], '%s_%s.overlay' % (
                user_id, app_id))
        if migrate is not None:
            overlay_response = urllib2.urlopen(
                migrate + '/residue?user_id=%s&app_id=%s' % (user_id, app_id))
            with open(overlay_path, 'wb') as stream:
                while True:
                    chunk = overlay_response.read(16 * 1024)
                    if not chunk:
                        break
                    stream.write(chunk)
        elif request.files.get('overlay'):
            overlay_file = request.files.get('overlay')
            overlay_file.save(overlay_path)
        elif request.values.get('overlay'):
            overlay_url = request.values.get('overlay')
            overlay_response = urllib2.urlopen(overlay_url)
            with open(overlay_path, 'wb') as stream:
                while True:
                    chunk = overlay_response.read(16 * 1024)
                    if not chunk:
                        break
                    stream.write(chunk)
        else:
            abort(400)
        net_started = start_network(network)
        if not net_started and migrate_network_info:
            app.logger.info(
                "failed migrating app '%s' for user '%s' because user already "
                "has a network assigned that will not match the required "
                "migration information for the migrating application",
                app_id, user_id)
            abort(400)
        if migrate_app_info:
            interface_mac = migrate_app_info['mac']
        else:
            interface_mac = random_mac()
        app.logger.info(
            "starting app '%s' for user '%s' on cloudlet server '%s' "
            "connected to network '%s' with MAC '%s'",
            app_id, user_id, selected_cloudlet['name'], network['network'],
            interface_mac)
        cloudlet_client = launch_vm(
            selected_cloudlet, network['network'],
            interface_mac, overlay_path)
        app.logger.info(
            "started app '%s' for user '%s' on cloudlet server '%s' "
            "connected to network '%s' with MAC '%s'",
            app_id, user_id, selected_cloudlet['name'], network['network'],
            interface_mac)
        app.logger.info(
            "waiting for app '%s' for user '%s' on cloudlet server '%s' "
            "connected to network '%s' with MAC '%s' to get an IP address",
            app_id, user_id, selected_cloudlet['name'], network['network'],
            interface_mac)
        if migrate_app_info:
            vm_ip = migrate_app_info['ip']
        else:
            vm_ip = wait_for_ip(network['network'], interface_mac)
        if not vm_ip:
            # Never got an IP address, so lets stop the client.
            app.logger.error(
                "failed waiting for IP address for app '%s' for user '%s' "
                "on cloudlet server '%s' connected to network '%s' "
                "with MAC '%s'",
                app_id, user_id, selected_cloudlet['name'],
                network['network'], interface_mac)
            cloudlet_client.terminate()
            if get_user_network(user_id) is None:
                stop_network(network)
            abort(400)
        network['apps'][app_id] = {
            'cloudlet': selected_cloudlet,
            'client': cloudlet_client,
            'mac': interface_mac,
            'ip': vm_ip,
        }
        return make_response(
            jsonify({
                'mac': interface_mac,
                'ip': vm_ip,
                'vpn': get_vpn_client_config(network['network']),
            }), 201)
    elif request.method == 'DELETE':
        network, user_app = get_network_and_app(user_id, app_id)
        user_app['client'].terminate()
        atomic_network_release(user_id, app_id)
        if get_user_network(user_id) is None:
            stop_network(network)
        return make_response('', 204)
    else:
        network, user_app = get_network_and_app(user_id, app_id)
        return jsonify({
            'mac': user_app['mac'],
            'ip': user_app['ip'],
            'vpn': get_vpn_client_config(network['network']),
        })


@app.route('/migrate', methods=['GET'])
def migrate():
    """
    Endpoint that is called by another gateway to start the migration on this
    gateway.
    """
    user_id = request.values.get("user_id")
    app_id = request.values.get("app_id")
    network, user_app = get_network_and_app(user_id, app_id)
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network['network']]
    vid = net_info['vid']
    if 'migration' in network:
        # The network was migrated to this cloudlet, so return VID the VM
        # is assigned in this VLAN.
        vid = network['migration']['vid']
    # See if migration has already started.
    migrate = 'inprogress'
    thread = user_app.get('migrate_thread', None)
    if thread:
        if not thread.isAlive():
            migrate = 'complete'
    else:
        # Start the migration.
        start_migration(user_app)
    return jsonify({
        'network': {
            'vid': vid,
            'leases': dump_leases(network['network']),
        },
        'app': {
            'mac': user_app['mac'],
            'ip': user_app['ip'],
        },
        'migrate': migrate,
    })


@app.route('/migrate/residue', methods=['GET'])
def migrate_residue():
    """
    Endpoint that is called by another gateway to get the residue from the
    migration.
    """
    user_id = request.values.get("user_id")
    app_id = request.values.get("app_id")
    network, user_app = get_network_and_app(user_id, app_id)
    thread = user_app.get('migrate_thread', None)
    if not thread or thread.isAlive():
        abort(400)
    residue_path = user_app['migrate_path']
    if not os.path.exists(residue_path):
        abort(400)
    def chunk_response():
        with open(residue_path, 'wb') as stream:
            while True:
                buf = stream.read(4096)
                if not buf:
                    os.unlink(residue_path)
                    os.rmdir(os.path.dirname(residue_path))
                    atomic_network_release(user_id, app_id)
                    if get_user_network(user_id) is None:
                        stop_network(network)
                    break
                yield buf
    return Response(chunk_response(), mimetype='application/octet-stream')


def run_server(
        config_file=None, overlays_path=None, debug=False,
        host=None, port=None):
    """Run the flask server."""
    if not config_file:
        config_file = os.path.join('/etc', 'cloudlet', 'config.yaml')
    if not overlays_path:
        overlays_path = os.path.join(
            '/var', 'lib', 'cloudlet', 'uploads', 'overlays')
    if not os.path.exists(config_file):
        raise MissingConfigError("'%s' doesn't exist." % config_file)
    with open(config_file, 'r') as stream:
        data = stream.read().strip()
        if not data:
            raise MissingConfigError("'%s' is empty." % config_file)
        config = yaml.load(data)
    if not os.path.exists(overlays_path):
        os.makedirs(overlays_path)
    app.config['CLOUDLET_CONFIG'] = config
    app.config['CLOUDLET_STATE'] = {}
    app.config['OVERLAYS_PATH'] = overlays_path
    app.run(debug=debug, host=host, port=port)


if __name__ == '__main__':
    main()
