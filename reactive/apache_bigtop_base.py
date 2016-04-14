import yaml
from charms.reactive import hook, when, when_not, set_state
from charms.layer.apache_bigtop_base import get_bigtop_base
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import role_and_interface_to_relations \
    as rel_names
from jujubigdata.handlers import HDFS, YARN
from jujubigdata import utils


HDFS_RELATION = (rel_names('requires', 'dfs') or
                 rel_names('provides', 'dfs-slave'))
YARN_RELATION = (rel_names('requires', 'mapred') or
                 rel_names('provides', 'mapred-slave'))


@hook('upgrade-charm')
def handle_legacy_installed_flag():
    bigtop = get_bigtop_base()
    if bigtop.is_installed():
        set_state('bigtop.installed')


if HDFS_RELATION:
    @when('bigtop.installed', '{hdfs}.joined'.format(hdfs=HDFS_RELATION[0]))
    def set_hdfs_spec(namenode):
        bigtop = get_bigtop_base()
        namenode.set_local_spec(bigtop.spec())

    @when('{hdfs}.spec.mismatch'.format(hdfs=HDFS_RELATION[0]))
    def hdfs_spec_mismatch(namenode):
        hookenv.status_set('blocked',
                           'Spec mismatch with NameNode: {} != {}'.format(
                               namenode.local_spec(), namenode.remote_spec()))

    @when('{hdfs}.ready'.format(hdfs=HDFS_RELATION[0]))
    def configure_hdfs(namenode):
        bigtop = get_bigtop_base()
        hdfs = HDFS(bigtop)
        utils.update_kv_hosts(namenode.hosts_map())
        utils.manage_etc_hosts()
        if not namenode.namenodes():
            data = yaml.dump({
                'relation_name': namenode.relation_name,
                'conversations': {
                    conv.key: dict({'relation_ids': conv.relation_ids},
                                   **conv.serialize(conv))
                    for conv in namenode.conversations()
                },
                'relation_data': {
                    rid: {
                        unit: hookenv.relation_get(unit=unit, rid=rid)
                        for unit in hookenv.related_units(rid)
                    } for rid in hookenv.relation_ids(namenode.relation_name)
                },
            }, default_flow_style=False)
            for line in data.splitlines():
                hookenv.log(line)
        hdfs.configure_hdfs_base(namenode.namenodes()[0], namenode.port())
        set_state('bigtop.hdfs.configured')


if YARN_RELATION:
    @when('bigtop.installed', '{yarn}.joined'.format(yarn=YARN_RELATION[0]))
    def set_yarn_spec(resourcemanager):
        bigtop = get_bigtop_base()
        resourcemanager.set_local_spec(bigtop.spec())

    @when('{yarn}.spec.mismatch'.format(yarn=YARN_RELATION[0]))
    def yarn_spec_mismatch(resourcemanager):
        hookenv.status_set(
            'blocked',
            'Spec mismatch with ResourceManager: {} != {}'.format(
                resourcemanager.local_spec(), resourcemanager.remote_spec()))

    @when('{yarn}.ready'.format(yarn=YARN_RELATION[0]))
    def configure_yarn(resourcemanager):
        bigtop = get_bigtop_base()
        yarn = YARN(bigtop)
        utils.update_kv_hosts(resourcemanager.hosts_map())
        utils.manage_etc_hosts()
        if not resourcemanager.resourcemanagers():
            data = yaml.dump({
                'relation_name': resourcemanager.relation_name,
                'conversations': {
                    conv.key: dict({'relation_ids': conv.relation_ids},
                                   **conv.serialize(conv))
                    for conv in resourcemanager.conversations()
                },
                'relation_data': {
                    rid: {
                        unit: hookenv.relation_get(unit=unit, rid=rid)
                        for unit in hookenv.related_units(rid)
                    } for rid in hookenv.relation_ids(
                        resourcemanager.relation_name
                    )
                },
            }, default_flow_style=False)
            for line in data.splitlines():
                hookenv.log(line)
        yarn.configure_yarn_base(
            resourcemanager.resourcemanagers()[0], resourcemanager.port(),
            resourcemanager.hs_http(), resourcemanager.hs_ipc())
        set_state('bigtop.yarn.configured')
