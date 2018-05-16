import os
import shutil
import requests
import json

import logging

log = logging.getLogger(__name__)


class connection(object):
    def __init__(self, url=None, session_id=None, app_name=None, user_id=None, username=None, password=None, key=None):

        self.url = url
        self.app_name = app_name
        self.session_id = session_id
        self.username = username
        self.user_id = user_id
        
        if username and password:
            self.login(username, password)

    def call(self, func, args):
        log.info("Calling: %s" % (func))
        headers = {'Content-Type': 'application/json',
                   'appname': self.app_name}

        data = json.dumps({func: args})

        repeat = True
        password = True
        i = 0
        while repeat and password is not None and i < 2:
            i += 1 # a precautionary check to prevent infinite loops
            repeat = False  # don't repeat unless we need to
            cookie = {'beaker.session.id': self.session_id, 'appname:': self.app_name}
            try:
                response = requests.post(self.url, data=data, headers=headers, cookies=cookie, timeout=100)
            except:
                return None
            content = response.content.decode()
            if response.ok:
                if self.session_id is None:
                    self.session_id = response.cookies['beaker.session.id']
                resp = json.loads(content, object_hook=JSONObject)
            else:
               resp = None

        return resp

    def login(self, username, password):
        self.session_id = None
        self.username = username
        response = self.call('login', {'username': username, 'password': password})

        return response

    def call2(self, fn, **kwargs):
        # convert any boolean parameters to 'Y' or 'N'.
        for kwarg in kwargs.keys():
            if type(kwargs[kwarg]) == bool:
                kwargs[kwarg] = 'Y' if kwargs[kwarg] is True else 'N'
        for item in ['project', 'network']:
            if item in kwargs and 'owners' in kwargs[item]:
                del kwargs[item]['owners']
        return self.call(fn, kwargs)

    # specific methods
    def get_user_by_name(self, username=None):
        resp = self.call('get_user_by_name', {'username': username})
        return resp

    def update_add_data_user(self, admin_username, admin_password, username, password):

        # login with admin account
        # conn = connection(url=data_url)
        self.login(username=admin_username, password=admin_password)

        data_user = self.call('get_user_by_name', {'username': username})
        if data_user:  # update it
            data_user = self.call2('update_user_password', user_id=data_user['id'], new_password=password)
        else:  # add it
            data_user = self.call2('add_user', user={'username': username, 'password': password})
        return data_user

    def update_user_password(self, user_id, password):
        self.call('update_user_password', {'user_id': user_id, 'new_password': password})

    def add_user(self, username, password):
        return self.call('add_user', {'user': {'username': username, 'password': password}})

    def get_project(self, project_id=None):
        return self.call('get_project', {'project_id': project_id})

    def get_projects(self, user_id=None):
        if user_id is None:
            return self.call('get_projects', {})
        else:
            return self.call('get_projects', {'user_id': user_id})

    def get_project_by_name(self, project_name=None):
        resp = self.call('get_project_by_name', {'project_name': project_name})
        return resp

    def get_network_by_name(self, project_id=None, network_name=None):
        resp = self.call('get_network_by_name', {'project_id': project_id, 'network_name': network_name})
        return resp

    def add_project(self, project):
        return self.call('add_project', {'project': project})

    def update_project(self, project):
        return self.call('update_project', {'project': project})

    def get_network(self, network_id, include_resources=True, summary=True, include_data=False):
        return self.call2('get_network',
                          network_id=network_id,
                          include_resources=include_resources,
                          summary=summary,
                          include_data=include_data
                          )

    def get_network_simple(self, network_id):
        return self.get_network(network_id=network_id, include_resources=False)

    def get_networks(self, project_id, include_resources=True, summary=False, include_data=False):
        return self.call('get_networks', {
            'project_id': project_id,
            'include_data': 'Y' if include_data else 'N',
            'include_resources': 'Y' if include_resources else 'N',
            'summary': 'Y' if summary else 'N'
        })

    def update_network(self, **kwargs):
        return self.call2('update_network', **kwargs)

    def get_scenarios(self, network_id=None):
        return self.call('get_scenarios', {'network_id': network_id})

    def get_template(self, template_id):
        return self.call('get_template', {'template_id': template_id})

    def get_template_from_network(self, network):
        template_id = self.get_template_id_from_network(network)
        return self.call2('get_template', template_id=template_id)

    def get_template_id_from_network(self, network):
        if 'active_template_id' in network.layout:
            template_id = network.layout.active_template_id
        else:
            template_id = network.types[0].template_id
            network['layout']['active_template_id'] = template_id
            self.call2('update_network', net=network)
        return template_id

    def get_template_name_from_network(self, network):
        if 'active_template_name' in network.layout:
            template_name = network.layout.active_template_name
        else:
            template_name = network.types[0].active_template_name
        return template_name

    def get_node(self, node_id=None):
        return self.call('get_node', {'node_id': node_id})

    def get_link(self, link_id=None):
        return self.call('get_link', {'link_id': link_id})

    def add_template_from_json(self, template, basename, version, dest):
        new_tpl = template.copy()
        new_tpl['name'] = '{}_v{}'.format(basename, version)

        # copy old template directory
        tpl_dir = dest
        src = os.path.join(tpl_dir, template['name'])
        dst = os.path.join(tpl_dir, new_tpl['name'])
        shutil.copytree(src, dst)

        # old_tpl = os.path.join(tpl_dir, 'template', 'template.xml')
        # if os.path.exists(old_tpl):
        # os.remove(old_tpl) # old xml is obsolete

        # genericize the template
        def visit(path, key, value):
            if key in {'cr_date'}:
                return False
            elif key in {'id', 'template_id', 'type_id', 'attr_id'}:
                return key, None
            return key, value

        new_tpl = remap(new_tpl, visit=visit)

        new_template = self.call('add_template', {'tmpl': new_tpl})

        return new_template

    def get_res_scen_data(self, **kwargs):
        res_scenario = self.call2(
            'get_resource_attribute_datasets',
            resource_attr_id=kwargs['res_attr_id'],
            scenario_id=kwargs['scenario_id']
        )
        return res_scenario

    def get_res_attr_data(self, **kwargs):
        res_attr_data = self.call2(
            'get_resource_attribute_data',
            ref_key=kwargs['ref_key'].upper(),
            ref_id=kwargs['ref_id'],
            scenario_id=kwargs['scenario_id'],
            attr_id=kwargs['attr_id'] if 'attr_id' in kwargs else None
        )
        return res_attr_data


class JSONObject(dict):
    def __init__(self, obj_dict):
        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)
