import os
import socket
import subprocess
import yaml

from path import Path

from charms import layer
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charmhelpers.core import hookenv
from charmhelpers.core.host import chdir


class Bigtop(object):

    def __init__(self):
        self.bigtop_dir = '/home/ubuntu/bigtop.release'
        self.options = layer.options('apache-bigtop-base')
        self.bigtop_version = self.options.get('bigtop_version')
        self.bigtop_base = Path(self.bigtop_dir) / self.bigtop_version

    def install(self, hosts, roles=None):
        '''Fetch, install, setup puppet config, and run puppet apply.'''
        self.fetch_bigtop_release()
        self.install_puppet_modules()
        self.setup_puppet(hosts, roles)
        self.trigger_puppet()

    def trigger_puppet(self):
        charm_dir = hookenv.charm_dir()
        # TODO JIRA KWM: rm does not need Hdfs_init and will fail
        rm_patch = Path(charm_dir) / 'resources/patch1_rm_init_hdfs.patch'
        # TODO JIRA KWM: nm should not *need* mapred role. we could patch it
        # with nm_patch, or adjust nm charm to include mapred role. for now,
        # we're doing the latter. todo rfc from dev@bigtop list.
        # nm_patch = Path(charm_dir) / 'resources/patch2_nm_core-site.patch'
        # TODO JIRA KWM: client role needs common_yarn for yarn-site.xml
        client_patch = Path(charm_dir) / 'resources/patch3_client_role_use_common_yarn.patch'
        with chdir("{}".format(self.bigtop_base)):
            # rm patch goes first
            utils.run_as('root', 'patch', '-p1', '-s', '-i', rm_patch)
            # skip nm_patch for now since nm charm is including mapred role
            # utils.run_as('root', 'patch', '-p1', '-s', '-i', nm_patch)
            # client patch goes last
            utils.run_as('root', 'patch', '-p1', '-s', '-i', client_patch)
        # TODO FIX ABOVE KWM

        # puppet apply needs to be ran where recipes were unpacked
        with chdir("{}".format(self.bigtop_base)):
            utils.run_as('root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')
        # If we can't reverse resolve the hostname (like on azure), support DN
        # registration by IP address.
        try:
            socket.gethostbyaddr(utils.resolve_private_address(hookenv.unit_private_ip()))
        except socket.herror:
            hdfs_site = Path('/etc/hadoop/conf/hdfs-site.xml')
            with utils.xmlpropmap_edit_in_place(hdfs_site) as props:
                props['dfs.namenode.datanode.registration.ip-hostname-check'] = 'false'

        # We know java7 has MAXHOSTNAMELEN of 64 char, so we cannot rely on
        # java to do a hostname lookup on clouds that have >64 char fqdns
        # (gce). Force short hostname (< 64 char) into /etc/hosts as workaround.
        # Better fix may be to move to java8. See http://paste.ubuntu.com/16230171/
        host_name = subprocess.check_output(['facter', 'hostname']).strip().decode()
        if host_name:
            sed_expr = "s/^127.0.0.1.*$/127.0.0.1 localhost %s/" % host_name
            subprocess.check_call(["sed", "-i", "%s" % sed_expr, "/etc/hosts"])

    def setup_hdfs(self):
        # TODO ubuntu user needs to be added to the upstream HDFS formating
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', '/user/ubuntu')
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', 'ubuntu', '/user/ubuntu')

    def spec(self):
        """Return spec for services that require compatibility checks.

        This must only be called after 'hadoop' is installed.
        """
        return {
            'vendor': 'bigtop',
            'hadoop': get_hadoop_version(),
        }

    def setup_puppet(self, hosts, roles=None):
        # generate site.yaml for either role or component based installation
        hiera_dst = self.options.get('bigtop_hiera_path')
        hiera_conf = self.options.get('bigtop_hiera_config')
        hiera_site_yaml = self.options.get('bigtop_hiera_siteyaml')
        bigtop_site_yaml = "{0}/{1}".format(self.bigtop_base, hiera_site_yaml)
        if roles is None:
            self.prepare_bigtop_component(bigtop_site_yaml, hosts)
        else:
            self.prepare_bigtop_role(bigtop_site_yaml, hosts, roles)
        # Now copy hiera.yaml to /etc/puppet & point hiera to use the above location as hieradata directory
        Path("{0}/{1}".format(self.bigtop_base, hiera_conf)).copy(hiera_dst)
        utils.re_edit_in_place(hiera_dst, {
            r'.*:datadir.*': "  :datadir: {0}/".format(os.path.dirname(bigtop_site_yaml)),
        })

    def install_puppet_modules(self):
        # Install required modules
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib')
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-apt')

    def fetch_bigtop_release(self):
        # download Bigtop release; unpack the recipes
        bigtop_url = self.options.get('bigtop_release_url')
        Path(self.bigtop_dir).rmtree_p()
        au = ArchiveUrlFetchHandler()
        au.install(bigtop_url, self.bigtop_dir)

    def prepare_bigtop_role(self, hr_conf, hosts={}, roles=None):
        java_package_name = self.options.get('java_package_name')
        bigtop_apt = self.options.get('bigtop_repo-{}'.format(utils.cpu_arch()))

        nn_host = ''
        rm_host = ''
        spark_host = ''
        zk_host = ''
        zk_quorum = ''
        for k, host in hosts.items():
            if k == 'namenode':
                nn_host = host
            elif k == 'resourcemanager':
                rm_host = host
            elif k == 'spark':
                spark_host = host
            elif k == 'zk':
                zk_host = host
            elif k == 'zk_quorum':
                zk_quorum = host

        yaml_data = {
            'bigtop::hadoop_head_node': nn_host,
            'bigtop::roles_enabled': True,
            'bigtop::roles': roles,
            'hadoop::common_hdfs::hadoop_namenode_host': nn_host,
            'hadoop::common_yarn::hadoop_ps_host': rm_host,
            'hadoop::common_yarn::hadoop_rm_host': rm_host,
            'hadoop::common_mapred_app::jobtracker_host': rm_host,
            'hadoop::common_mapred_app::mapreduce_jobhistory_host': rm_host,
            'hadoop::zk': zk_host,
            'spark::common::master_host': spark_host,
            'hadoop_zookeeper::server::ensemble': zk_quorum,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
            'bigtop::jdk_package_name': '{0}'.format(java_package_name),
            'bigtop::bigtop_repo_uri': '{0}'.format(bigtop_apt),
        }

        Path(hr_conf).dirname().makedirs_p()
        with open(hr_conf, 'w+') as fd:
            yaml.dump(yaml_data, fd)

    def prepare_bigtop_component(self, hr_conf, hosts={}):
        '''
        :param hosts: dict of {service: fqdn}
        '''
        cluster_components = self.options.get("bigtop_component_list").split()
        java_package_name = self.options.get('java_package_name')
        bigtop_apt = self.options.get('bigtop_repo-{}'.format(utils.cpu_arch()))
        gw_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
        nn_host = ''
        rm_host = ''
        for k, host in hosts.items():
            if k == 'namenode':
                nn_host = host
            elif k == 'resourcemanager':
                rm_host = host

        yaml_data = {
            'bigtop::hadoop_gateway_node': gw_host,
            'bigtop::hadoop_head_node': nn_host,
            'hadoop::common_yarn::hadoop_rm_host': rm_host,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
            'hadoop_cluster_node::cluster_components': cluster_components,
            'bigtop::jdk_package_name': '{0}'.format(java_package_name),
            'bigtop::bigtop_repo_uri': '{0}'.format(bigtop_apt),
        }

        Path(hr_conf).dirname().makedirs_p()
        with open(hr_conf, 'w+') as fd:
            yaml.dump(yaml_data, fd)


def get_bigtop_base():
    return Bigtop()


def get_hadoop_version():
    hadoop_out = subprocess.check_output(['hadoop', 'version']).decode()
    version = hadoop_out.split('\n')[0].split()[1]
    return version


def get_layer_opts():
    return utils.DistConfig(data=layer.options('apache-bigtop-base'))
