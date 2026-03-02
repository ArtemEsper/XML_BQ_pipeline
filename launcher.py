"""Flex Template entry point.

The Flex Template launcher runs this file as a plain script, so it cannot
use relative imports.  Since wos_beam_pipeline is installed as a package
(via pip install -e .), we can import it with an absolute import instead.
"""
from wos_beam_pipeline.main import run

if __name__ == '__main__':
    run()
