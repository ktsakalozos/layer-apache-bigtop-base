import yaml
import os

import subprocess
from subprocess import CalledProcessError
from path import Path

from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charmhelpers.core import unitdata, hookenv
from charmhelpers.core.host import chdir

class Bigtop(object):
    def is_installed(self):
        return unitdata.kv().get('bigtop.installed')

    def install(self, NN=None, RM=None, force=False):
        if not force and self.is_installed():
            return

        # download Bigtop release; unpack the recipes
        bigtop_dir = '/home/ubuntu/bigtop.release'
        bigtop_url = hookenv.config('bigtop_1.1.0_release')
        if not unitdata.kv().get('bigtop-release.installed', False):
            Path(bigtop_dir).rmtree_p()
            au = ArchiveUrlFetchHandler()
            au.install(bigtop_url, bigtop_dir)

            unitdata.kv().set('bigtop-release.installed', True)
            unitdata.kv().flush(True)

        # install required puppet modules
        try:
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
        except CalledProcessError:
            pass # All modules are set

        # generate site.yaml. Something like this would do
        hiera_dst = hookenv.config('bigtop_hiera_path')
        hiera_conf = hookenv.config('bigtop_hiera_config')
        bigtop_version = hookenv.config('bigtop_version')
        hiera_site_yaml = hookenv.config('bigtop_hiera_siteyaml')
        bigtop_site_yaml = "{0}/{1}/{2}".format(bigtop_dir, bigtop_version, hiera_site_yaml)

        prepare_bigtop_config(bigtop_site_yaml, NN, RM)

        # Now copy hiera.yaml to /etc/puppet & point hiera to use the above location as hieradata directory
        Path("{0}/{1}/{2}".format(bigtop_dir, bigtop_version, hiera_conf)).copy(hiera_dst)
        utils.re_edit_in_place(hiera_dst, {
            r'.*:datadir.*': "  :datadir: {0}/".format(os.path.dirname(bigtop_site_yaml)),
        })

        # TODO need to either manage the apt keys from Juju of
        # update upstream Puppet recipes to install them along with apt source
        # puppet apply needs to be ran where recipes were unpacked
        try:
            with chdir("{0}/{1}".format(bigtop_dir, bigtop_version)):
                utils.run_as('root', 'puppet', 'apply', '-d',
                             '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                             'bigtop-deploy/puppet/manifests/site.pp')
        except CalledProcessError:
            pass  # Everything seems to be fine

        try:
            utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', '/user/ubuntu')
            utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', 'ubuntu', '/user/ubuntu')
        except CalledProcessError:
            # TODO ubuntu user needs to be added to the upstream HDFS formating
            pass # The home directory for ubuntu user is created

        unitdata.kv().set('bigtop.installed', True)
        unitdata.kv().flush(True)

    # TODO no clear how to control the life-cycle of all other daemons in the stack
    # shall we writing separate charms for each of them?
    # this is how the life-cycle management is done
    # lib.jujubigdata.jujubigdata.handlers.HDFS#_hadoop_daemon
    # TODO the answer is to have separate charms and handle life-cycle inside of them


def prepare_bigtop_config(hr_conf, NN=None, RM=None):
    # TODO storage dirs should be configurable
    # TODO list of cluster components should be configurable
    localhost = subprocess.check_output(['hostname', '-f']).strip().decode()
    if NN == None:
        nn_hostname = localhost
    else:
        nn_hostname = NN
    if RM == None:
        rm_hostname = localhost
    else:
        rm_hostname = RM
    java_package_name = hookenv.config('java_package_name')
    # TODO figure out how to distinguish between different platforms
    bigtop_apt = hookenv.config('bigtop_1.1.0_repo-x86_64')

    yaml_data = {
        'bigtop::hadoop_head_node': nn_hostname,
        'hadoop::common_yarn::hadoop_rm_host': rm_hostname,
        'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
        'hadoop_cluster_node::cluster_components': ['yarn'],
        'bigtop::jdk_package_name': '{0}'.format(java_package_name),
        'bigtop::bigtop_repo_uri': '{0}'.format(bigtop_apt),
    }

    with safe_open(hr_conf, 'w+') as fd:
        yaml.dump(yaml_data, fd)


def get_bigtop_base():
    return Bigtop()


# Same as open, but making sure the path exists for the target file
def safe_open(path, mode):
    Path(path).dirname().makedirs_p()
    return open(path, mode)
