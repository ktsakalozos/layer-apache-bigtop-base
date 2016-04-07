from subprocess import CalledProcessError
from path import Path

from jujubigdata.utils import DistConfig
from jujubigdata.handlers import BigtopBase

from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils


class Bigtop(object):
    def __init__(self, dist_config):
        self.dist_config = dist_config

    def is_installed(sefl):
        return unitdata.kv().get('bigtop.installed')

    def install(self, force=false):
        if not force and self.is_installed():
            return

        # download Bigtop release; unpack the recipes
        bigtop_dir = '/home/ubuntu/bigtop.release'
        if not unitdata.kv().get('bigtop-release.installed', False):
            Path(bigtop_dir).rmtree_p()
            au = ArchiveUrlFetchHandler()
            au.install(bigtop_dir, '/home/ubuntu')

            unitdata.kv().set('bigtop-release.installed', True)
            unitdata.kv().flush(True)

        hiera_dst = self.dist_config.bigtop_hiera('path')
        hiera_conf = self.dist_config.bigtop_hiera('source')
        utils.re_edit_in_place(hiera_conf, {
            r'.*:datadir.*': '{}/{}'.format(hiera_dst, hiera_conf),
        })

        # generate site.yaml. Something like this would do
        # TODO how to get the name of he headnode?
        # TODO storage dirs should be configurable
        # TODO list of cluster components should be configurable
        # TODO shall we be installing JDK or let Juju do it?
        # TODO repo url is available through the config, so all's good
        """
        bigtop::hadoop_head_node: "hadoopmaster.example.com"
        hadoop::hadoop_storage_dirs:
          - "/data/1"
          - "/data/2"
        hadoop_cluster_node::cluster_components:
          - ignite_hadoop
          - spark
          - yarn
          - zookeeper
        bigtop::jdk_package_name: "openjdk-7-jre-headless"
        bigtop::bigtop_repo_uri: "http://bigtop-repos.s3.amazonaws.com/releases/1.1.0/ubuntu/trusty/x86_64"
        """
        # install required puppet modules
        try:
            utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib', 'puppetlabs-apt')
        except CalledProcessError:
            pass # All modules are set

        try:
            utils.run_as('root', 'root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')
        except CalledProcessError:
            pass  # Everything seems to be fine

        unitdata.kv().set('bigtop.installed', True)
        unitdata.kv().flush(True)

    def setup_bigtop_config(self):
        return

    # TODO no clear how to control the life-cycle of all other daemons in the stack
    # shall we writing separate charms for each of them?
    # this is how the life-cycle management is done
    # lib.jujubigdata.jujubigdata.handlers.HDFS#_hadoop_daemon
    # TODO the answer is to have separate charms and handle life-cycle inside of them

    def get_dist_config(required_keys=None):
        required_keys = required_keys or [
            'vendor', 'hadoop_version', 'packages',
            'groups', 'users', 'dirs', 'ports']
        dist = DistConfig(filename='dist.yaml',
                          required_keys=required_keys)
        opts = layer.options('apache-bigtop-base')
        for key in ('hadoop_version',):
            if key in opts:
                dist.dist_config[key] = opts[key]
        for key in ('packages', 'groups'):
            if key in opts:
                dist.dist_config[key] = list(
                    set(dist.dist_config[key]) | set(opts[key])
                )
        for key in ('users', 'dirs', 'ports'):
            if key in opts:
                dist.dist_config[key].update(opts[key])
        return dist

    def get_bigtop_base():
        return BigtopBase(get_dist_config())
