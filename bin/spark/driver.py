import os
import importlib
import sys



def main():
    filename = sys.argv[1]
    sys.argv.remove(filename)

    module = os.path.splitext(filename)[0]
    module = importlib.import_module(module)
    module.main()


if __name__ == '__main__':
    main()