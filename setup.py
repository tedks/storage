from setuptools import setup, find_packages

setup(
    name='storage',
    version='0.1.0',
    description='A meta-filesystem that multiplexes over multiple backing filesystems',
    license='AGPL-3.0-or-later',
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
    ],
    packages=find_packages(),
    install_requires=[
        'fusepy>=3.0.1',
        'PyYAML>=6.0',
        'click>=8.0.0',
        'xxhash>=3.0.0',
    ],
    entry_points={
        'console_scripts': [
            'storage=storage.cli:main',
        ],
    },
    python_requires='>=3.7',
)
