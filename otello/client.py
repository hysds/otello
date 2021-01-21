import os
import yaml
import getpass
from pathlib import Path


def initialize():
    """initialize .cfg file

    prompt for user input:
    1. check ~/.config/otello/otello.cfg if it exists
    2. prompt user for HySDS host (Mozart IP or DNS)
       - if it exists in config.yml then use existing value if not supplied by user
    3. prompt user for username and password: https://docs.python.org/3/library/getpass.html
       - will make request to SSO provider to retrieve access and refresh token
       - ******* need to implement SSO/Auth first *******
    4. create .cfg file
       - cfg file will have: HySDS host, access_token, refresh_token, token expiration time
    """

    cfg_dir = os.path.join(str(Path.home()), '.config/otello')
    cfg_file = os.path.join(cfg_dir, 'config.yml')  # ~/.config/otello/config.yml

    if not os.path.exists(cfg_dir):
        os.makedirs(cfg_dir)
        print('created path %s\n' % cfg_dir)

    config = {}
    try:
        with open(cfg_file, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print('%s not found\n' % cfg_file)
    except yaml.YAMLError:
        print('unable to load ~/.config/otello/config.yml\n')
    except Exception as e:
        print(e)

    # HySDS host
    existing_host = config.get('host')
    host_prompt = 'HySDS host (current value: %s): ' % existing_host if existing_host else 'HySDS host: '
    host = input(host_prompt)
    if host:
        config['host'] = host

    is_auth = input('HySDS cluster authenticated (y/n): ')
    if is_auth.lower() == 'y':
        config['auth'] = True

        # Username
        existing_user = config.get('username')
        user_prompt = 'Username (current value: %s): ' % existing_user if existing_user else 'Username: '
        username = input(user_prompt)
        if username:
            config['username'] = username

        # Password
        password = getpass.getpass()
        # TODO: use username + password to retrieve access_token and refresh_token from SSO provider
    else:
        config['auth'] = False

    print('\n' + yaml.dump(config))

    with open(cfg_file, 'w') as f:
        yaml.dump(config, f)
