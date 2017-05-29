`GFS.py`
========

Python 3 implementation of a date-based [Grandfather-father-son backup rotation
scheme](https://en.wikipedia.org/wiki/Backup_rotation_scheme#Grandfather-father-son).

This script takes a list of dates (in the form `yyyy-mm-ddThh:mm:ss`, or other
parseable with the `strfime(3)` format) and returns the dates that should be
kept (or removed, using `--remove`) under the policy given by the positional
arguments passed to the script.

It is also possible to integrate `gfs.py` as a regular Python module.

Requirements
------------

- [Python](https://www.python.org/) 3.6+

Example
-------

```
python3 gfs.py daily=14 weekly=4 montly=3 yearly=2
```

Usage
-----

```
$ python3 gfs.py --help
usage: gfs.py [-h] [--date-format FORMAT] [--file FILENAME]
              [--keep | --remove]
              cycle=value [cycle=value ...]

Filter dates using the Grandfather-father-son rotation scheme.

positional arguments:
  cycle=value           Set one or more cycles used for rotation, e.g.,
                        daily=14.

optional arguments:
  -h, --help            show this help message and exit
  --date-format FORMAT, -d FORMAT
                        Date format to use to parse the dates (in strftime(3)
                        format). Default:%Y-%m-%dT%H:%M:%S
  --file FILENAME, -f FILENAME
                        Use filename to read a list of dates instead of the
                        standard input; '-' is interpreted as the standard
                        input.
  --keep, -k            Output the dates to keep in the rotation.
  --remove, -r          Output the dates to remove from the rotation.
```

License
-------

See `LICENSE`.
