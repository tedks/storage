#!/usr/bin/env python3

import argparse

def main():
    parser = argparse.ArgumentParser(description='Storage meta-filesystem tool')
    subparsers = parser.add_subparsers(dest='command')

    mount_parser = subparsers.add_parser('mount', help='Mount a storage filesystem')
    mount_parser.add_argument('config', help='Path to the storagetab.yaml file')
    mount_parser.add_argument('mountpoint', help='Path to the mountpoint directory')

    args = parser.parse_args()

    if args.command == 'mount':
        from storage_lib import Storage
        storage = Storage(args.config)
        storage.mount(args.mountpoint)
        print("Listing files in the root directory:")
        files = storage.list_files('/')
        for f in files:
            print(f"  - {f}")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
