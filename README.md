# pyrcmemcached

memcached-like library based on IRCv3.2

How to use:

```
git clone https://github.com/ProgVal/pyrcmemcached.git
cd pyrcmemcached/
git submodule init
git submodule update
pip3 install --user ircmatch ircreactor PyYAML # Mammon's dependencies
./mammon/mammond --config mammon/mammond.yml
./pyrcmemcached.py # Run tests
```

Now import `pyrcmemcached`, and use it. Its interface is based on
[python-memcached](https://github.com/linsomniac/python-memcached).

