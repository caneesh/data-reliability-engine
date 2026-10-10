"""spark-submit entry point for dre: passes the command line to `dre` (hcsc.datalake.dre.cli).

    spark-submit --py-files dre-pyfiles-<version>.zip dre_main.py run --conf /path/to/conf
"""

import sys

from hcsc.datalake.dre.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
