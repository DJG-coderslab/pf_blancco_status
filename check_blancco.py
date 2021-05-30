import argparse
import dateutil.parser
import ntplib
import requests
import subprocess
import datetime
import mysql.connector as sqldb

from art import *
from colorconsole import terminal
from configparser import ConfigParser
from datetime import timedelta, datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

from numpy.lib import format
from requests import ConnectionError

bg = terminal.colors['LGREY']
screen = terminal.get_terminal(conEmu=False)
screen.set_title('Check Blancco status')
screen.set_color(terminal.colors['WHITE'], bg)
screen.clear()
requests.packages.urllib3.disable_warnings()

app_version = '1.7.1.2'

disks = {}
reports = {}
caseID_dict = {}
sn_disk_dict = {}
error = True
error_description = ''
threshold_clean = timedelta(37)
ntp_server = '10.61.12.22'
desc_error = '---'
config_path = ''
exit_ok = 0
exit_error = 8
is_optane = False
local_url = 'https://blancco.service.acer-euro.com:8443/rest-service/report/export/xml'
cloud_url = 'https://cloud.blancco.com:443/rest-service/report/export/xml'

if getattr(sys, 'frozen', False):
    config_path = Path(sys.executable).parent
elif __file__:
    config_path = Path(__file__).parent
ini_file = config_path / 'blancco.ini'
if ini_file.is_file():
    parser = ConfigParser()
    try:
        parser.read(ini_file)
        ntp_server = parser.get('ntp', 'server')
    except OSError:
        print("Can't read {}".format(ini_file))


def get_reports(value, url=cloud_url, method='caseid', aspect='amazon'):
    """
    Funkcja pobierająca raporty z serwera blancco.
    Raporty moga byc pobierane wg zadanego CaseID (domyślnie) lub
    sn dysku.
    Parametr method określa sposób wybierania raportów caseUID lub sn_disk
    Paramter value zawiera caseID lub serial number dysku
    """
    global desc_error
    if aspect == "amazon":
        if debug: print("AMAZON!!!")
        _user = "***REMOVED***"
        _user_pass = "***REMOVED***"
    else:
        if debug: print("ASPLEX!!!")
        _user = '***REMOVED***'
        _user_pass = '***REMOVED***'
    _APIUrl = url
    if method == 'caseid':
        _xml_path = 'user_data.fields.CASEID'
    elif method == 'sn_disk':
        _xml_path = 'blancco_data.blancco_hardware_report.disks.disk.serial'
    else:
        print('ERROR, unknown method!')
        sys.exit(exit_error)
    _xml_value = value
    _request = '''<?xml version="1.0" encoding="UTF-8"?> 
                    <request> 
                        <export-report>  
                            <report mode="original"/> 
                            <search path="{path}" value="{value}" 
                                operator="eq" 
                                datatype="string" 
                                conjunction="true" /> 
                        </export-report>
                    </request>'''.format(path=_xml_path, value=_xml_value)
    _files = [('xmlRequest', _request)]
    _reports = requests.post(_APIUrl, files=_files, verify=False,
                             auth=(_user, _user_pass)).text
    if 'MC_EXPORT_REPORT_FAILED' in _reports:
        desc_error = 'Not found any report...'
        _reports = {}
    return _reports


def parse_xml(xml):
    """
        z serwera blancco przychodzi jeden plik/strumień ze wszystkimi
        raportami o danym caseID lub SN dysku

        Funkcja wypełnia słownik reports{} polami:
            document_id jako klucz główny, dla każedego klucza:
                date_LUN     - data (chyba) z czyszczonego systemu
                date_CM      - data (chyba) serwera, Managment Console
                staete       - wynik czyszczenia
                disk_model   - model dysku z raportu
                disk_serial  - serial dysku z raportu
                caseID       - caseID z raportu
    """
    if debug:
        print('parse_xml')
    _xml = xml
    _root = ET.fromstring(_xml)
    _blancco_id = 'not_know'
    _date_LUN = 'nn'
    _date_MC = 'nn'
    _state = 'nn'
    _serial_disk = 'nn'
    _model_disk = 'nn'
    _caseID = 'nn'
    _duration = 'nn'
    for _n in _root.iter('report'):
        for _m in _n.iter('description'):
            for _entry in _m.findall('document_id'):
                if _entry.text is not None:
                    _blancco_id = _entry.text
                    reports[_blancco_id] = {}
            for _entry in _m[1][0].findall('date'):
                _date_LUN = _entry.text
            for _entry in _m[1][1].findall('date'):
                _date_MC = _entry.text
        for _m in _n.iter('blancco_erasure_report'):
            for _entry in _m[0][0].findall('entry'):
                if _entry.attrib['name'] == 'state':
                    _state = _entry.text
                if _entry.attrib['name'] == 'elapsed_time':
                    _duration = _entry.text
            for _entries in _m.iter('entries'):
                if _entries.attrib['name'] == 'target':
                    for _entry in _entries.findall('entry'):
                        if _entry.attrib['name'] == 'model':
                            _model_disk = _entry.text
                        if _entry.attrib['name'] == 'serial':
                            _serial_disk = _entry.text
        for _m in _n.iter('user_data'):
            for _entries in _m.iter('entries'):
                for _entry in _entries.findall('entry'):
                    if _entry.attrib['name'] == 'CASEID':
                        _caseID = _entry.text.upper()
        # if _blancco_id not in reports:
        #     print('\t\tFilling the report')
        reports.update({_blancco_id: {'date_LUN': _date_LUN,
                                      'date_MC': _date_MC,
                                      'state': _state,
                                      'disk_model': _model_disk,
                                      'disk_serial': _serial_disk,
                                      'caseID': _caseID}})
        if _caseID not in caseID_dict:
            caseID_dict[_caseID] = []
        caseID_dict[_caseID].append(
            [_serial_disk, _state, _date_LUN, _date_MC, _model_disk,
             _duration])
        if _serial_disk not in sn_disk_dict:
            sn_disk_dict[_serial_disk] = []
        sn_disk_dict[_serial_disk].append(
            [_caseID, _state, _date_LUN, _date_MC, _model_disk, _duration])
    if debug:
        print('\treports: ')
        for _key in reports:
            print('\t\t{} {}'.format(_key, reports[_key]))
        print('\tcaseID:')
        for _key in caseID_dict:
            print('\t\t{} {}'.format(_key, caseID_dict[_key]))
        print('\tSN: ')
        for _key in sn_disk_dict:
            print('\t\t{} {}'.format(_key, sn_disk_dict[_key]))


def get_disks():
    global is_optane
    _length = 1
    _wmic = subprocess.Popen(
        ['wmic', 'diskdrive', 'get', 'SerialNumber,', 'Caption'],
        stdout=subprocess.PIPE, shell=True)
    (_output, _error) = _wmic.communicate()
    _output = _output.decode("utf-8")
    _lines = (_output.split('\r\r\n'))
    if debug:
        print(_lines)
        hit_key()
    for _line in _lines:
        if 'SerialNumber' in _line:
            _length = (len(_line.split('Serial')[0]))
        else:
            _caption = _line[:_length].strip()
            _sn_disk = _line[_length:].strip()
            """ exclude for Optane Memory
                
                SN OptaneMemory for all devices is Optane_000 
                so it's a lot of reports in blancco
                checked only in --check mode
            """
            if silent and 'Optane_0000' in _sn_disk:
                is_optane = True
                continue
            if len(_sn_disk) > 0:
                disks[_sn_disk] = {}
                disks[_sn_disk]['name'] = _caption


def check_status_device():
    if debug:
        print('check_status_device:')
    global desc_error
    """
        returns True if even one report is Successful
    """
    for _disk in disks:
        if debug:
            print('\tdisks: {}'.format(disks))
        _xml = get_reports(_disk, url=cloud_url, method='sn_disk')
        if not _xml:
            _xml = get_reports(_disk, url=local_url, method='sn_disk')
        if _xml:
            parse_xml(_xml)
    for _sn_disk, _lists in sn_disk_dict.items():
        if debug:
            print('\tSN: {} list: {}'.format(_sn_disk, _lists))
            hit_key()
        if _sn_disk not in disks:
            '''
            skip disks from reports that there aren't in device
            '''
            continue
        for _list in _lists:
            _clean_date = dateutil.parser.parse(_list[3])
            if 'Successful' in _list \
                    and now_date() - _clean_date < threshold_clean:
                """
                    The good cleaning was less than 37 days ago
                """
                return True
        desc_error = 'Check if the Successful was in 37 last days'
        return False


def check_status_caseID(_caseID:str):
    if debug:
        print('check_status_caseID:')
    global desc_error
    _xml = get_reports(_caseID, url=cloud_url,
                       method='caseid', aspect='amazon')
    if _xml: parse_xml(_xml)
    _xml = get_reports(_caseID, url=cloud_url,
                       method='caseid', aspect='asplex')
    if _xml: parse_xml(_xml)
    # if _xml:
    #     # TODO writing the xml should be move to other place
    #     if debug:
    #         try:
    #             print("writing the report")
    #             with open ('report.xml', 'w') as f:
    #                 print(_xml, file=f)
    #         except IOError:
    #             print("Error while writing the report")
    #     parse_xml(_xml)
    if reports:
        if db:
            handling_db()
        for _report, _report_dict in reports.items():
            if 'Successful' not in _report_dict['state']:
                if debug:
                    print("\tNot successful: {}".format(_report))
                    hit_key()
                continue
            _clean_date = dateutil.parser.parse(_report_dict['date_LUN'])
            if now_date() - _clean_date < threshold_clean:
                """
                    The good cleaning was less than 14 days ago
                """
                if debug:
                    print("\t{}: {}".format(_report, _report_dict['caseID']))
                    hit_key()
                return True
            else:
                if debug:
                    print("Success but more than 14 day ago")
        desc_error = 'Check if the Successful was in 14 last days'
    return False


def hit_key():
    print('Hit any key...')
    screen.getch()


def now_date():
    _client = ntplib.NTPClient()
    _now = ''
    try:
        _response = _client.request(ntp_server)
        _now = datetime.fromtimestamp(_response.tx_time, timezone.utc)
    except ConnectionError:
        _now = datetime.now(timezone.utc)
        screen.set_color(terminal.colors['YELLOW'], bg)
        print(' WARNING')
        print('\n Problem with NTP server: {}'.format(ntp_server))
        print(' Use the system date: {}'.format(_now))
        screen.set_color(terminal.colors['WHITE'], bg)
    if debug:
        print("now is: ", end='')
        print(_now)
    return _now


def print_version():
    print('Version: {}'.format(app_version))


def display_status_device():
    if debug:
        print(disks)
        hit_key()
    if check_status_device():
        screen.clear()
        screen.set_color(10, bg)
        print(text2art('  SUCCESSFUL!'))
        screen.set_color(15, bg)
    else:
        screen.clear()
        screen.set_color(4, bg)
        print(text2art('  FAIL!!!'))
        screen.set_color(terminal.colors['BLUE'], bg)
        print(desc_error)
        print()
    for _sn_disk, _lists in sn_disk_dict.items():
        for _list in _lists:
            screen.set_color(terminal.colors['PURPLE'], bg)
            print('\t{:10}{}'.format('CaseID:', _list[0]))
            if 'Successful' in _list[1]:
                screen.set_color(terminal.colors['GREEN'], bg)
            else:
                screen.set_color(terminal.colors['RED'], bg)
            print('\t{:10}{}'.format('Status:', _list[1]))
            screen.set_color(terminal.colors['YELLOW'], bg)
            print('\t{:10}{}, sn: {}'.format('Disk:', _list[4], _sn_disk))
            screen.set_color(terminal.colors['BLACK'], bg)
            print(
                '\t{:10}{}\n\t{:10}{}'.format('Date:', _list[3], 'Duration:',
                                              _list[5]))
            print('\n')
    screen.set_color(11, bg)
    print('Hit any key...')
    screen.reset_colors()
    screen.getch()
    # Waits for a single key touch before ending.
    screen.set_color(terminal.colors['WHITE'], terminal.colors['BLACK'])
    screen.clear()
    sys.exit(exit_ok)


def handling_db():
    for _report, _reports_dict in reports.items():
        if not is_into_db(_report):
            _sql = create_insert(_report, **_reports_dict)
            insert_report(_sql)


def insert_report(sql):
    try:
        _sql_connection = sqldb.connect(user='bl', password='3wagty5r5g',
                                        database='blancco_reports',
                                        host='10.61.12.21')
        _cursor = _sql_connection.cursor()
        _cursor.execute(sql)
        _sql_connection.commit()
    except  sqldb.Error as e:
        if debug:
            print("Error")
            print(e)


def create_insert(document_id, date_LUN, date_MC, state, disk_model,
                  disk_serial, caseID):
    _tmp= datetime.strptime(date_LUN, "%Y-%m-%dT%H:%M:%S%z")
    _date_LUN = _tmp.strftime('%Y-%m-%d %H:%M:%S')
    _tmp = datetime.strptime(date_MC, "%Y-%m-%dT%H:%M:%S%z")
    _date_MC = _tmp.strftime('%Y-%m-%d %H:%M:%S')
    _now = datetime.now()
    _sql = """INSERT INTO reports (document_id, caseID, state, LUN_date,
          MC_date, local_timestamp, disk_model, disk_serial)
          VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")""".\
        format(document_id, caseID, state, _date_LUN, _date_MC, _now,
               disk_model,disk_serial)
    return _sql


def is_into_db(_document_id):
    _cursor = None
    try:
        _sql_connection = sqldb.connect(user='bl', password='3wagty5r5g',
                                        database='blancco_reports',
                                        host='10.61.12.21')
        _cursor = _sql_connection.cursor()
        _sql = """SELECT document_id FROM reports WHERE document_id="{}" """\
            .format(_document_id)
        _cursor.execute(_sql)
    except  sqldb.Error as e:
        if debug:
            print("Error")
            print(e)
        return True     # True, because there isn't sense inserting into DB
    for i in _cursor:
        return True if i else False


""" ========================================================================= 
                              START PROGRAM
    =========================================================================
"""
parser = argparse.ArgumentParser(description='Application for checking '
                                             'Blancco status')

parser.add_argument('-s', '--silent', action='store_true',
                    help='Silent check status, no display any things to '
                         'display, return only 0 for OK or 8 if false')
parser.add_argument('-i', '--caseid', action='store', type=str,
                    help='Interactive search by CaseID')
parser.add_argument('-o', '--optane', action='store_true',
                    help='Return OK (0) if in device there is OptaneMemory')
parser.add_argument('-v', '--version', action='store_true',
                    help='Print version')
parser.add_argument('-d', '--debug', action='store_true',
                    help=argparse.SUPPRESS)
parser.add_argument('-b', '--database', action='store_true',
                    help='handling DB')

args = parser.parse_args()
silent = args.silent
debug = args.debug
skip_optane = args.optane
case_id = args.caseid
version = args.version
db = args.database

if version:
    print_version()
    now_date()
    sys.exit()

screen.set_color(1, bg)
print(text2art(' START CHECKING'))
print(text2art('   BLANCCO...'))

get_disks()

if is_optane and skip_optane:
    if debug:
        print('There is Optane and -o')
    sys.exit(exit_ok)

if not silent:
    if case_id:
        if check_status_caseID(case_id):
            if debug:
                print('The device was correct cleaned')
                hit_key()
            screen.set_color(terminal.colors['WHITE'], terminal.colors['BLACK'])
            screen.clear()
            sys.exit(exit_ok)
        else:
            if debug:
                print("Not OK, the device wasn't correct cleaned by 14 days ago")
                hit_key()
            screen.set_color(terminal.colors['WHITE'], terminal.colors['BLACK'])
            screen.clear()
            sys.exit(exit_error)
    else:
        display_status_device()
else:
    if debug:
        print("Check -c {}".format(disks))
        hit_key()
    if check_status_device():
        if debug:
            print('OK')
            hit_key()
        screen.set_color(terminal.colors['WHITE'], terminal.colors['BLACK'])
        screen.clear()
        sys.exit(exit_ok)
    else:
        if debug:
            print('Not OK')
            hit_key()
        screen.set_color(terminal.colors['WHITE'], terminal.colors['BLACK'])
        screen.clear()
        sys.exit(exit_error)
