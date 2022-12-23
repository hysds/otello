import os
import yaml

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

    # Username
    existing_user = config.get('username')
    user_prompt = 'Username (current value: %s): ' % existing_user if existing_user else 'Username: '
    username = input(user_prompt)
    if username:
        config['username'] = username
    else:
        if existing_user:
            config['username'] = existing_user
        else:
            raise RuntimeError("Please input user")

    is_auth = input('HySDS cluster authenticated (y/n): ')
    if is_auth.lower() == 'y':
        config['auth'] = True
        # Using AWS Secrets Manager for authentication
        # Current assumption is that the Secret ID will be equal to the username.
        # If not, end user will change it.
        existing_secret_id = config.get('aws_secret_id', config["username"])
        user_prompt = f"AWS Secrets Manager ID (current value: {existing_secret_id}): "
        secret_id = input(user_prompt)
        if secret_id:
            config['aws_secret_id'] = secret_id
        else:
            config['aws_secret_id'] = existing_secret_id
    else:
        config['auth'] = False

    print('\n' + yaml.dump(config))

    with open(cfg_file, 'w') as f:
        yaml.dump(config, f)
