import os
import yaml

from pathlib import Path
from otello.auth import get_authentication_token


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
    host = config.get('host')
    host_prompt = 'HySDS host (current value: %s): ' % host if host else 'HySDS host: '
    host_input = input(host_prompt)
    config['host'] = host_input or host
    if not config['host']:
        raise RuntimeError("Please input HySDS host")

    # Username
    username = config.get('username')
    user_prompt = 'Username (current value: %s): ' % username if username else 'Username: '
    username_input = input(user_prompt)
    config['username'] = username_input or username
    if not config['username']:
        raise RuntimeError("Please input user")

    is_auth = input('HySDS cluster authenticated (y/n): ')
    if is_auth.lower() == 'y':
        config['auth'] = True
        config['token'] = get_authentication_token(config['host'], config['username'])
    else:
        config['auth'] = False

    print('\n' + yaml.dump(config))

    with open(cfg_file, 'w') as f:
        yaml.dump(config, f)
