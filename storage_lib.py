import yaml
import os

class Storage:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def mount(self, mountpoint):
        print(f"Mounting filesystem at {mountpoint}")
        print("Backing filesystems:")
        for fs in self.config['filesystems']:
            print(f"  - {fs['path']}")

    def list_files(self, path):
        print(f"Listing files in {path}")
        all_files = []
        for fs in self.config['filesystems']:
            # In a real implementation, we would check the extended attributes
            # to see which filesystem stores which directory.
            # For this prototype, we'll just list all files from all filesystems.
            try:
                files = os.listdir(fs['path'])
                all_files.extend(files)
            except FileNotFoundError:
                print(f"Warning: {fs['path']} not found.")
        return all_files
