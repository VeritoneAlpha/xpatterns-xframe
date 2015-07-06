#!/usr/bin/python

import os, sys
import argparse

# Find the code that controls ipython menus and patch it to include XFrame documentation.

def patch_ipkernel(ipkernel_path):
    with open(ipkernel_path, 'r') as f:
        with open(ipkernel_path + '.out', 'w') as wf:
            for line in f:
                if line.strip() == "'text': \"IPython\",":
                    wf.write(line.replace('\n', '') + '  #patched\n')
                    wf.write(f.next())
                    close_brace = f.next()
                    wf.write(close_brace)
                    prefix = close_brace.replace('},', '')
                    prefix = prefix.replace('\n', '')
                    wf.write(prefix + '{\n')
                    wf.write(prefix + '    ' + '\'text\': "XFrame",\n')
                    wf.write(prefix + '    ' + '\'url\': "http://localhost:8000",\n')
                    wf.write(prefix + '},\n')
                elif line.strip() ==  "'text': \"IPython\",  #patched":
                    wf.write(line)   # text
                    line = f.next()  # url
                    wf.write(line)
                    line = f.next()  # close brace
                    wf.write(line)
                    line = f.next()  # open brace
                    wf.write(line)
                    line = f.next()  # text
                    wf.write(line)
                    line = f.next()  # url
                    index = line.find('"')
                    if index == -1:
                        raise ValueError('update patched: unexpected url {}'.format(line))
                    new_line = line[0:index] + '\"http://localhost:8000/"\n'
                    wf.write(new_line)
                else:
                    wf.write(line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Patch help_links.')
    parser.add_argument('venv', type=str, help='path to the virtual environment')
    args = parser.parse_args()
    if not os.path.isdir(args.venv):
        print '{} is not a directory'.format(args.venv)
        sys.exit(1)
    ipkernel_path = os.path.join(args.venv, 'lib/python2.7/dist-packages/IPython/kernel/zmq/ipkernel.py')
    if not os.path.isfile(ipkernel_path):
        print 'ipkernel file not found: {}'.format(ipkernel_path)
        sys.exit(1)
    patch_ipkernel(ipkernel_path)
    os.remove(ipkernel_path)
    os.rename(ipkernel_path + '.out', ipkernel_path)
    exit(0)
    
