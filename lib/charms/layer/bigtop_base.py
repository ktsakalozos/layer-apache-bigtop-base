from subprocess import CalledProcessError

from jujubigdata.utils import DistConfig
from jujubigdata.handlers import HadoopBase

from lib.jujubigdata.jujubigdata import utils


class Bigtop(object):
    def __init__(self):
## get the resoures somehow

    def is_installed(sefl):
        return unitdata.kv().get('bigtop.installed')

    def install(self, force=false):
        if not force and self.is_installed():
            return

        # download Bigtop release; unpack the recipes

        try:
            utils.run_as('root', 'root', 'tar', 'zxf', 'bigtop-1.1.0-project.tar.gz', 'bigtop-deploy')
        except CalledProcessError:
            pass # No exceptions, assuming everything was unpacked nicely

        # configure Hiera
        # generate site.yaml

        try:
            utils.run_as('root', 'root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')
        except CalledProcessError
            pass  # Everything seems to be fine

        unitdata.kv().set('bigtop.installed', True)
        unitdata.kv().flush(True)

    def setup_bigtop_config(self):
        return

    # TODO no clear how to control the life-cycle of all other daemons in the stack
    # shall we writing separate charms for each of them?
    # this is how the life-cycle management is done
    # lib.jujubigdata.jujubigdata.handlers.HDFS#_hadoop_daemon

