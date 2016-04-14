import yaml
import os

import subprocess
from path import Path

from charms import layer
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charmhelpers.core.host import chdir


class Bigtop(object):

    def __init__(self):
        self.bigtop_dir = '/home/ubuntu/bigtop.release'
        self.options = layer.options('apache-bigtop-base')
        self.bigtop_version = self.options.get('bigtop_version')

    def install(self, NN=None, RM=None):
        self.fetch_bigtop_release()
        self.install_puppet()
        self.setup_puppet_config(NN, RM)
        self.trigger_puppet()
        self.setup_hdfs()

    def trigger_puppet(self):
        # TODO need to either manage the apt keys from Juju of
        # update upstream Puppet recipes to install them along with apt source
        # puppet apply needs to be ran where recipes were unpacked
        with chdir("{0}/{1}".format(self.bigtop_dir, self.bigtop_version)):
            utils.run_as('root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')

    def setup_hdfs(self):
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', '/user/ubuntu')
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', 'ubuntu', '/user/ubuntu')

    def setup_puppet_config(self, NN, RM):
        # generate site.yaml. Something like this would do
        hiera_dst = self.options.get('bigtop_hiera_path')
        hiera_conf = self.options.get('bigtop_hiera_config')
        hiera_site_yaml = self.options.get('bigtop_hiera_siteyaml')
        bigtop_site_yaml = "{0}/{1}/{2}".format(self.bigtop_dir, self.bigtop_version, hiera_site_yaml)
        self.prepare_bigtop_config(bigtop_site_yaml, NN, RM)
        # Now copy hiera.yaml to /etc/puppet & point hiera to use the above location as hieradata directory
        Path("{0}/{1}/{2}".format(self.bigtop_dir, self.bigtop_version, hiera_conf)).copy(hiera_dst)
        utils.re_edit_in_place(hiera_dst, {
            r'.*:datadir.*': "  :datadir: {0}/".format(os.path.dirname(bigtop_site_yaml)),
        })

    def install_puppet(self):
        # BIGTOP-2003. A workaround to install newer hiera to get rid of hiera 1.3.0 bug.
        # TODO once Ubuntu has fixed version of hiera package, this could be replaced by
        # adding packages: ['puppet'] in layer.yaml options:basic
        wget = 'wget -O /tmp/puppetlabs-release-trusty.deb https://apt.puppetlabs.com/puppetlabs-release-trusty.deb'
        dpkg = 'dpkg -i /tmp/puppetlabs-release-trusty.deb'
        apt_update = 'apt-get update'
        puppet_install = 'apt-get install --yes puppet'
        subprocess.call(wget.split())
        subprocess.call(dpkg.split())
        subprocess.call(apt_update.split())
        subprocess.call(puppet_install.split())
        # Install required modules
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib')
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-apt')

    def fetch_bigtop_release(self):
        # download Bigtop release; unpack the recipes
        bigtop_url = self.options.get('bigtop_release_url')
        Path(self.bigtop_dir).rmtree_p()
        au = ArchiveUrlFetchHandler()
        au.install(bigtop_url, self.bigtop_dir)

    def prepare_bigtop_config(self, hr_conf, NN, RM):
        # TODO storage dirs should be configurable
        # TODO list of cluster components should be configurable
        localhost = subprocess.check_output(['hostname', '-f']).strip().decode()
        if NN is None:
            nn_hostname = localhost
        else:
            nn_hostname = NN
        if RM is None:
            rm_hostname = localhost
        else:
            rm_hostname = RM
        java_package_name = self.options.get('java_package_name')
        # TODO figure out how to distinguish between different platforms
        bigtop_apt = self.options.get('bigtop_1.1.0_repo-x86_64')

        yaml_data = {
            'bigtop::hadoop_head_node': nn_hostname,
            'hadoop::common_yarn::hadoop_rm_host': rm_hostname,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
            'hadoop_cluster_node::cluster_components': ['yarn'],
            'bigtop::jdk_package_name': '{0}'.format(java_package_name),
            'bigtop::bigtop_repo_uri': '{0}'.format(bigtop_apt),
        }

        Path(hr_conf).dirname().makedirs_p()
        with open(hr_conf, 'w+') as fd:
            yaml.dump(yaml_data, fd)


def get_bigtop_base():
    return Bigtop()
