from charms.reactive import when_not, set_state
from charms.layer.apache_bigtop_base import Bigtop


@when_not('bigtop.installed')
def install_hadoop():
    bigtop = Bigtop()
    bigtop.install()
    set_state('bigtop.installed')
