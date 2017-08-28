import re

from iso639 import languages
from sqlalchemy.sql import select, and_

from ckan.lib.dictization import model_dictize
import ckan.model as model

PID_TO_NAME_REGEXES = [r'[^A-Za-z0-9]', r'-']     # [pattern, replace]


def convert_language(lang):
    '''
    Convert alpha2 language (eg. 'en') to terminology language (eg. 'eng')
    '''

    if not lang:
        return "und"

    try:
        lang_object = languages.get(part1=lang)
        return lang_object.terminology
    except KeyError as ke:
        try:
            lang_object = languages.get(part2b=lang)
            return lang_object.terminology
        except KeyError as ke:
            return ''


# Should this be in some mapper or refiner?
def pid_to_name(string):
    '''
    Wrap re.sub to convert a PID to package.name.
    '''
    if string:
        return re.sub(PID_TO_NAME_REGEXES, string=string)


######## Temporary PID functions until using the harvest source guid as in CSCETSIN-51.  ######
def get_pids_by_type(pid_type, data_dict, relation=None):
    '''
    Get all package PID dicts of certain type

    :param pid_type: PID type to get (primary, relation)
    :param data_dict:
    :param relation: relation type. None == get all pids. Basically applicable only
            when pid type is relation, otherwise not useful since primary type does
            not have relation defined.
    :rtype: list of dicts
    '''

    return [x for x in data_dict.get('pids', {}) if x.get('type') == pid_type and
            (relation == None or x.get('relation') == relation)]


def get_primary_pid(data_dict, get_as_dict=False):
    '''
    Returns the primary PID of the given type for a package.
    This is a convenience function that returns the first primary PID
    returned by get_pids_by_type.

    If no primary PID can be found, this function returns None.

    :param data_dict:
    :param get_as_dict: If true, return a dictionary, otherwise return plaing string
    :return: the primary identifier of the package
    '''

    pids = get_pids_by_type(pid_type='primary', data_dict=data_dict)
    if pids:
        if get_as_dict:
            return pids[0]
        return pids[0]['id']
    else:
        return None


def get_package_id_by_pid(pid, pid_type):
    """ Find pid by id and type.

    :param pid: id of the pid
    :param pid_type: type of the pid (primary, relation)
    :return: id of the package
    """
    query = select(['key', 'package_id']).where(and_(model.PackageExtra.value == pid, model.PackageExtra.key.like('pids_%_id'),
                                                     model.PackageExtra.state == 'active'))

    for key, package_id in [('pids_%s_type' % key.split('_')[1], package_id) for key, package_id in model.Session.execute(query)]:
        query = select(['package_id']).where(and_(model.PackageExtra.value == pid_type, model.PackageExtra.key == key,
                                                  model.PackageExtra.state == 'active', model.PackageExtra.package_id == package_id))
        for package_id, in model.Session.execute(query):
            return package_id

    return None


def get_package_id_by_primary_pid(data_dict):
    '''
    Try if the provided primary PID matches exactly one dataset.

    THIS METHOD WAS PREVIOUSLY GET_PACKAGE_ID_BY_DATA_PIDS, is the below correct, or should relation pids also be used?

    :param data_dict:
    :return: Package id or None if not found.
    '''
    primary_pid = get_primary_pid(data_dict)
    if not primary_pid:
        return None

    pid_list = [primary_pid]

    # Get package ID's with matching PIDS
    query = model.Session.query(model.PackageExtra.package_id.distinct()).\
        filter(model.PackageExtra.value.in_(pid_list))
    pkg_ids = query.all()
    if len(pkg_ids) != 1:
        return None              # Nothing to do if we get many or zero datasets

    # Get extras with the received package ID's
    query = select(['key', 'value', 'state']).where(
        and_(model.PackageExtra.package_id.in_(pkg_ids), model.PackageExtra.key.like('pids_%')))

    extras = model.Session.execute(query)

    # Dictize the results
    extras = model_dictize.extras_list_dictize(extras, {'model': model.PackageExtra})

    # Check that matching PIDS are type 'primary'.
    for extra in extras:
        key = extra['key'].split('_')   # eg. ['pids', '0', 'id']

        if key[2] == 'id' and extra['value'] in pid_list:
            type_key = '_'.join(key[:2] + ['type'])

            if not filter(lambda x: x['key'] == type_key and (x['value'] == 'primary'), extras):
                return None      # Found a hit with wrong type of PID

    return pkg_ids[0]    # No problems found, so use this


def generate_pid():
    """
    Generate a permanent Kata identifier
    """
    import datetime
    return "urn:nbn:fi:csc-kata%s" % datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

######## Temporary PID functions end.  ######