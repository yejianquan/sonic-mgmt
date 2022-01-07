import sys
import time
import threading
import Queue
import yaml
import json
import random
import logging
import tempfile
import traceback

from collections import OrderedDict
from natsort import natsorted
from netaddr import IPNetwork
from test_vrf_t1_lag import setup_vrf                                                               # lgtm[py/unused-import]

import pytest

from tests.common import config_reload
from tests.common.fixtures.ptfhost_utils import copy_ptftests_directory                             # lgtm[py/unused-import]
from tests.common.fixtures.ptfhost_utils import change_mac_addresses                                # lgtm[py/unused-import]
from tests.common.platform.processes_utils import wait_critical_processes
from tests.common.storage_backend.backend_utils import skip_test_module_over_backend_topologies     # lgtm[py/unused-import]
from tests.ptf_runner import ptf_runner
from tests.common.utilities import wait_until
from tests.common.reboot import reboot

"""
    During vrf testing, a vrf basic configuration need to be setup before any tests,
    and cleanup after all tests. Both of the two tasks should be called only once.

    A module-scoped fixture `setup_vrf` is added to accompilsh the setup/cleanup tasks.
    We want to use ansible_adhoc/tbinfo fixtures during the setup/cleanup stages, but
        1. Injecting fixtures to xunit-style setup/teardown functions is not support by
            [now](https://github.com/pytest-dev/pytest/issues/5289).
        2. Calling a fixture function directly is deprecated.
    So, we prefer a fixture rather than xunit-style setup/teardown functions.
"""

pytestmark = [
    pytest.mark.topology('t1-lag')
]

logger = logging.getLogger(__name__)

# global variables
g_vars = {}
PTF_TEST_PORT_MAP = '/root/ptf_test_port_map.json'

# helper functions
def get_pc_members(portchannel_name, cfg_facts):
    tmp_member_list = []

    for m in cfg_facts['PORTCHANNEL_MEMBER'].keys():
        pc, port = m.split('|')
        if portchannel_name == pc:
            tmp_member_list.append(port)

    return natsorted(tmp_member_list)

def get_intf_ips(interface_name, cfg_facts):
    prefix_to_intf_table_map = {
        'PortChannel': 'PORTCHANNEL_INTERFACE',
        'Ethernet': 'INTERFACE',
        'Loopback': 'LOOPBACK_INTERFACE'
    }

    intf_table_name = None

    ip_facts = {
        'ipv4': [],
        'ipv6': []
    }

    for pfx, t_name in prefix_to_intf_table_map.iteritems():
        if pfx in interface_name:
            intf_table_name = t_name
            break

    if intf_table_name is None:
        return ip_facts

    for intf in cfg_facts[intf_table_name]:
        if '|' in intf:
            if_name, ip = intf.split('|')
            if if_name == interface_name:
                ip = IPNetwork(ip)
                if ip.version == 4:
                    ip_facts['ipv4'].append(ip)
                else:
                    ip_facts['ipv6'].append(ip)

    return ip_facts

def get_cfg_facts(duthost):
    tmp_facts = json.loads(duthost.shell("sonic-cfggen -d --print-data")['stdout'])  # return config db contents(running-config)

    port_name_list_sorted = natsorted(tmp_facts['PORT'].keys())
    port_index_map = {}
    for idx, val in enumerate(port_name_list_sorted):
        port_index_map[val] = idx

    tmp_facts['config_port_indices'] = port_index_map

    return tmp_facts

def get_vrf_intfs(cfg_facts):
    intf_tables = ['INTERFACE', 'PORTCHANNEL_INTERFACE', 'LOOPBACK_INTERFACE']
    vrf_intfs = {}

    for table in intf_tables:
        for intf, attrs in cfg_facts.get(table, {}).iteritems():
            if '|' not in intf:
                vrf = attrs['vrf_name']
                if vrf not in vrf_intfs:
                    vrf_intfs[vrf] = {}
                vrf_intfs[vrf][intf] = get_intf_ips(intf, cfg_facts)

    return vrf_intfs

def get_vrf_ports(cfg_facts):
    '''
    :return: vrf_member_port_indices, vrf_intf_member_port_indices
    '''

    pc_member = cfg_facts['PORTCHANNEL_MEMBER'].keys()
    member = pc_member

    vrf_intf_member_port_indices = {}
    vrf_member_port_indices = {}

    vrf_intfs = get_vrf_intfs(cfg_facts)

    for vrf, intfs in vrf_intfs.iteritems():
        vrf_intf_member_port_indices[vrf] = {}
        vrf_member_port_indices[vrf] = []

        for intf in intfs:
            vrf_intf_member_port_indices[vrf][intf] = natsorted(
                    [ cfg_facts['config_port_indices'][m.split('|')[1]] for m in filter(lambda m: intf in m, member) ]
                )
            vrf_member_port_indices[vrf].extend(vrf_intf_member_port_indices[vrf][intf])

        vrf_member_port_indices[vrf] = natsorted(vrf_member_port_indices[vrf])

    return vrf_intf_member_port_indices, vrf_member_port_indices

def ex_ptf_runner(ptf_runner, exc_queue, **kwargs):
    '''
    With this simple warpper function, we could use a Queue to store the
    exception infos and check it later in main thread.

    Example:
        refer to test 'test_vrf_swss_warm_reboot'
    '''
    try:
        ptf_runner(**kwargs)
    except Exception:
        exc_queue.put(sys.exc_info())

def finalize_warmboot(duthost, comp_list=None, retry=30, interval=5):
    '''
    Check if componets finish warmboot(reconciled).
    '''
    DEFAULT_COMPONENT_LIST = ['orchagent', 'neighsyncd']
    EXP_STATE = 'reconciled'

    comp_list = comp_list or DEFAULT_COMPONENT_LIST

    # wait up to $retry * $interval secs
    for _ in range(retry):
        for comp in comp_list:
            state =  duthost.shell('/usr/bin/redis-cli -n 6 hget "WARM_RESTART_TABLE|{}" state'.format(comp), module_ignore_errors=True)['stdout']
            logger.info("{} : {}".format(comp, state))
            if EXP_STATE == state:
                comp_list.remove(comp)
        if len(comp_list) == 0:
            break
        time.sleep(interval)
        logger.info("Slept {} seconds!".format(interval))

    return  comp_list

def check_interface_status(duthost, up_ports):
    intf_facts = duthost.interface_facts(up_ports=up_ports)['ansible_facts']
    if len(intf_facts['ansible_interface_link_down_ports']) != 0:
        logger.info("Some ports went down: {} ...".format(intf_facts['ansible_interface_link_down_ports']))
        return False
    return True

def check_bgp_peer_state(duthost, vrf, peer_ip, expected_state):
    peer_info = json.loads(duthost.shell("vtysh -c 'show bgp vrf {} neighbors {} json'".format(vrf, peer_ip))['stdout'])

    logger.debug("Vrf {} bgp peer {} infos: {}".format(vrf, peer_ip, peer_info))

    try:
        peer_state = peer_info[peer_ip].get('bgpState', 'Unknown')
    except Exception as e:
        peer_state = 'Unknown'
    if  peer_state != expected_state:
        logger.info("Vrf {} bgp peer {} is {}, exptected {}!".format(vrf, peer_ip, peer_state, expected_state))
        return False

    return True

def check_bgp_facts(duthost, cfg_facts):
    result = {}
    for neigh in cfg_facts['BGP_NEIGHBOR']:
        if '|' not in neigh:
            vrf = 'default'
            peer_ip = neigh
        else:
            vrf, peer_ip = neigh.split('|')

        result[(vrf, peer_ip)] = check_bgp_peer_state(duthost, vrf, peer_ip, expected_state='Established')

    return all(result.values())

def setup_vrf_cfg(duthost, localhost, cfg_facts):
    '''
    setup vrf configuration on dut before test suite
    '''

    # FIXME
    # For vrf testing, we should create a new vrf topology
    # might named to be 't0-vrf', deploy with minigraph templates.
    #
    # But currently vrf related schema does not properly define in minigraph.
    # So we generate and deploy vrf basic configuration with a vrf jinja2 template,
    # later should move to minigraph or a better way(VRF and BGP cli).

    from copy import deepcopy
    cfg_t0 = deepcopy(cfg_facts)

    cfg_t0.pop('config_port_indices', None)

    extra_vars = {'cfg_t0': cfg_t0}

    duthost.host.options['variable_manager'].extra_vars.update(extra_vars)

    duthost.template(src="vrf/vrf_config_db_t1_lag.j2", dest="/tmp/config_db_vrf.json")
    #duthost.shell("cp /tmp/config_db_vrf.json /etc/sonic/config_db.json")
    logging.info("render_finished")
    time.sleep(600)
    config_reload(duthost)
    wait_critical_processes(duthost)


def gen_vrf_fib_file(vrf, tbinfo, ptfhost, render_file, dst_intfs=None, \
                     limited_podset_number=10, limited_tor_number=10):
    dst_intfs = dst_intfs if dst_intfs else get_default_vrf_fib_dst_intfs(vrf, tbinfo)
    extra_vars = {
        'testbed_type': tbinfo['topo']['name'],
        'props': g_vars['props'],
        'intf_member_indices': g_vars['vrf_intf_member_port_indices'][vrf],
        'dst_intfs': dst_intfs,
        'limited_podset_number': limited_podset_number,
        'limited_tor_number': limited_tor_number
    }

    ptfhost.host.options['variable_manager'].extra_vars.update(extra_vars)

    ptfhost.template(src="vrf/vrf_fib.j2", dest=render_file)

def get_default_vrf_fib_dst_intfs(vrf, tbinfo):
    '''
    Get default vrf fib destination interfaces(PortChannels) according to the given vrf.
    The test configuration is dynamic and can work with 4 and 8 PCs as the number of VMs.
    The first half of PCs are related to Vrf1 and the second to Vrf2.
    '''
    dst_intfs = []
    vms_num = len(tbinfo['topo']['properties']['topology']['VMs'])
    if vrf == 'Vrf1':
        dst_intfs_range = list(range(1, int(vms_num / 2) + 1))
    else:
        dst_intfs_range = list(range(int(vms_num / 2) + 1, vms_num + 1))
    for intfs_num in dst_intfs_range:
        dst_intfs.append('PortChannel000{}'.format(intfs_num))

    return dst_intfs

def gen_vrf_neigh_file(vrf, ptfhost, render_file):
    extra_vars = {
        'intf_member_indices': g_vars['vrf_intf_member_port_indices'][vrf],
        'intf_ips': g_vars['vrf_intfs'][vrf]
    }

    ptfhost.host.options['variable_manager'].extra_vars.update(extra_vars)

    ptfhost.template(src="vrf/vrf_neigh.j2", dest=render_file)

def gen_specific_neigh_file(dst_ips, dst_ports, render_file, ptfhost):
    dst_ports = [str(port) for port_list in dst_ports for port in port_list]
    tmp_file = tempfile.NamedTemporaryFile()
    for ip in dst_ips:
        tmp_file.write('{} [{}]\n'.format(ip, ' '.join(dst_ports)))
    tmp_file.flush()
    ptfhost.copy(src=tmp_file.name, dest=render_file)

# For dualtor
def get_dut_enabled_ptf_ports(tbinfo, hostname):
    dut_index = str(tbinfo['duts_map'][hostname])
    ptf_ports = set(tbinfo['topo']['ptf_map'][dut_index].values())
    disabled_ports = set()
    if dut_index in tbinfo['topo']['ptf_map_disabled']:
        disabled_ports = set(tbinfo['topo']['ptf_map_disabled'][dut_index].values())
    return ptf_ports - disabled_ports

# fixtures

@pytest.fixture(scope="module")
def dut_facts(duthosts, rand_one_dut_hostname):
    duthost = duthosts[rand_one_dut_hostname]
    return duthost.facts


@pytest.fixture(scope="module")
def cfg_facts(duthosts, rand_one_dut_hostname):
    duthost = duthosts[rand_one_dut_hostname]
    return get_cfg_facts(duthost)

def restore_config_db(localhost, duthost):
    # In case something went wrong in previous reboot, wait until the DUT is accessible to ensure that
    # the `mv /etc/sonic/config_db.json.bak /etc/sonic/config_db.json` is executed on DUT.
    # If the DUT is still inaccessible after timeout, we may have already lose the DUT. Something sad happened.
    localhost.wait_for(host=g_vars["dut_ip"],
                        port=22,
                        state='started',
                        search_regex='OpenSSH_[\\w\\.]+ Debian',
                        timeout=180)   # Similiar approach to increase the chance that the next line get executed.
    duthost.shell("mv /etc/sonic/config_db.json.bak /etc/sonic/config_db.json")
    reboot(duthost, localhost)

@pytest.fixture(scope="module", autouse=True)
def setup_vrf(tbinfo, duthosts, rand_one_dut_hostname, ptfhost, localhost, skip_test_module_over_backend_topologies):
    duthost = duthosts[rand_one_dut_hostname]

    # backup config_db.json
    duthost.shell("mv /etc/sonic/config_db.json /etc/sonic/config_db.json.bak")

    ## Setup global variables
    global g_vars

    try:
        ## Setup dut
        g_vars["dut_ip"] = duthost.host.options["inventory_manager"].get_host(duthost.hostname).vars["ansible_host"]
        duthost.critical_services = ["swss", "syncd", "database", "teamd", "bgp"]  # Don't care about 'pmon' and 'lldp' here
        cfg_t0 = get_cfg_facts(duthost)  # generate cfg_facts for t0 topo

        setup_vrf_cfg(duthost, localhost, cfg_t0)

        # Generate cfg_facts for t0-vrf topo, should not use cfg_facts fixture here. Otherwise, the cfg_facts
        # fixture will be executed before setup_vrf and will have the original non-VRF config facts.
        cfg_facts = get_cfg_facts(duthost)

        duthost.shell("sonic-clear arp")
        duthost.shell("sonic-clear nd")
        duthost.shell("sonic-clear fdb all")

        with open("../ansible/vars/topo_{}.yml".format(tbinfo['topo']['name']), 'r') as fh:
            g_vars['topo_properties'] = yaml.safe_load(fh)

        g_vars['props'] = g_vars['topo_properties']['configuration_properties']['common']

        g_vars['vrf_intfs'] = get_vrf_intfs(cfg_facts)

        g_vars['vrf_intf_member_port_indices'], g_vars['vrf_member_port_indices'] = get_vrf_ports(cfg_facts)

    except Exception as e:
        # Ensure that config_db is restored.
        # If exception is raised in setup, the teardown code won't be executed. That's why we need to capture
        # exception and do cleanup here in setup part (code before 'yield').
        logger.error("Exception raised in setup: {}".format(repr(e)))
        logger.error(json.dumps(traceback.format_exception(*sys.exc_info()), indent=2))

        restore_config_db(localhost, duthost, ptfhost)

        # Setup failed. There is no point to continue running the cases.
        pytest.fail("VRF testing setup failed")    # If this line is hit, script execution will stop here

    # --------------------- Testing -----------------------
    yield

    # --------------------- Teardown -----------------------
    restore_config_db(localhost, duthost, ptfhost)

@pytest.fixture(scope="module")
def mg_facts(duthosts, rand_one_dut_hostname, tbinfo):
    duthost = duthosts[rand_one_dut_hostname]
    mg_facts = duthost.get_extended_minigraph_facts(tbinfo)
    return mg_facts


# tests
class TestVrfScaling():
    def test_vrf_in_kernel(self, duthosts, rand_one_dut_hostname, cfg_facts):
        duthost = duthosts[rand_one_dut_hostname]
        # verify vrf in kernel
        res = duthost.shell("ip link show type vrf | grep Vrf")

        for vrf in cfg_facts['VRF'].keys():
            assert vrf in res['stdout'], "%s should be created in kernel!" % vrf

        for vrf, intfs in g_vars['vrf_intfs'].iteritems():
            for intf in intfs:
                res = duthost.shell("ip link show %s" % intf)
                assert vrf in res['stdout'], "The master dev of interface %s should be %s !" % (intf, vrf)
