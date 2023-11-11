import sys, json, csv, datetime
import pandas as pd
from dataclasses import dataclass
from ruamel.yaml import YAML
from dateutil.relativedelta import relativedelta
import os.path
from os import path

import logging

logger = logging.getLogger('glrule_logic')

currentDir = os.path.dirname(os.path.realpath(__file__))
logger.info(currentDir)

@dataclass
class DebetOrCredit:
    def __init__(self, account, varname, varname2=None, varnamelist=None, dim=None, Memo=None):
        self.account = account
        self.varname = varname
        self.varnamelist = varnamelist
        self.dim = dim
        self.Memo = Memo
        if varname2 is None:
            self.varname2 = "default"
        else:    
            self.varname2 = varname2
        #print(f"Account {account} varname {varname} varname2 {varname2}")

    def multiply(self, par):
        pass

    def Process(self, data):
        stot = data.get(self.varname, 0)
        if hasattr(self, 'varname2'):
            s = data.get(self.varname2)
            if s is not None:
                stot += float(s)
        if stot == 0:
            return None
        account = None
        if hasattr(self, 'account'):
            account = data.get(self.account, self.account)
        if account is None:
            account = data.get('Account')
        ret = { "Account" : account, "Amount" : self.multiply(stot) }
        if hasattr(self, 'dim'):    
           ret["Dim1"] = data.get(self.dim)
        ret["Memo"] = data.get("Memo", "xx")
        return ret

@dataclass
class Debet(DebetOrCredit):
    def __init__(self, account, varname=None, varname2=None, varnamelist=None, dim=None, Memo=None):
        super().__init__(account, varname, varname2=varname2, varnamelist=varnamelist, dim=dim, Memo=Memo)
    
    def multiply(self, s):
        return float(s)

@dataclass
class Credit(DebetOrCredit):
    def __init__(self, account, varname, varname2=None, dim=None, Memo=None):
        super().__init__(account, varname, varname2=varname2, dim=dim, Memo=Memo)

    def multiply(self, s):
        return -1 * float(s)

@dataclass
class GlRule:
    def __init__(self, code, name, prefix, *debetOrCredit):
        self.code = code
        self.name = name
        self.prefix = prefix
        self.debetOrCredit = debetOrCredit

    def parsePvm(self, date_time_str):
        pvmFormat = '%Y-%m-%d' if "-" in date_time_str else '%d.%m.%Y'
        try:
            dPvm = datetime.datetime.strptime(date_time_str, pvmFormat)
        except Exception as inst:
            logger.error(inst, f"Tarkista päiväys {date_time_str}")
            raise
        return dPvm

    def ProcessRule(self, data):
        ret = []
        amountTot = 0
        for d in self.debetOrCredit:
            #print(d)
            transaction = d.Process(data)
            if transaction is not None and 'Amount' in transaction:
                dPvm = self.parsePvm(data["Date"])
                transaction["Date"] = dPvm
                transaction["Memo"] = data.get("Memo","")
                transaction["Journal"] = self.prefix + dPvm.strftime('%y-%m')
                ret.append(transaction)
                amountTot = amountTot + transaction['Amount']
        if amountTot != 0:
            logger.error(f"*** Transaction total {amountTot}, {data}")
        return ret

def load_rules() -> dict:
    listRules = [
    GlRule(10, "Myynti", "ML", Credit('3000','Amount'), Credit('2915', 'Tax' ), Debet( '1700', varnamelist= [ 'Amount', 'Tax' ] )),
    GlRule(21, "Palkka", "PLK", Credit('5000','tulo'), Credit('5140', 'sotu' ), Debet( '2911', 'ennpid'), Debet( '2930', 'maksettu' ), Debet( '2984', 'sotu' ))
    ]

    #yaml = YAML()
    yaml = YAML(typ="safe", pure=True)
    yaml.register_class(DebetOrCredit)
    yaml.register_class(Debet)
    yaml.register_class(Credit)
    yaml.register_class(GlRule)

    with open(f"{currentDir}/glrules.yml") as file:
        listRules = yaml.load(file)

    #yaml.dump(listRules, sys.stdout)
    dictRules = {}
    for r in listRules:
        dictRules[r.code] = r
    return dictRules

def lueFile(filename):
    """
    Compute the sum of squares of a list of numbers.ls 
    Args:
        nums (`list` of `int` or `float`): A `list` of numbers.
    Returns:
        ans (`int` or `float`): Sum of squares of `nums`.
    Raises:
        AssertionError: If `nums` contain elements that are not floats nor ints.
    """    
    with open(filename, "r", encoding='latin-1') as read_file:
        for line in read_file: 
            #print(line)
            if line[0] != "#" and "," in line:
                json_data = "{" + line.replace("'", '"') + "}"
                try:
                    parsed_json = (json.loads(json_data))
                    listData.append(parsed_json)
                except json.decoder.JSONDecodeError as ve:
                    logger.error(ve)
                    logger.error(json_data)
                    raise