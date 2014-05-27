#!/usr/local/bin/python python
# encoding: utf-8

import win32com.client
import sys
import datetime

############################
#  Constants for blpQuery  #
############################

ADMIN = 1
AUTHORIZATION_STATUS = 11
BLPSERVICE_STATUS = 9
PARTIAL_RESPONSE = 6
PUBLISHING_DATA = 13
REQUEST_STATUS = 4
RESOLUTION_STATUS = 12
RESPONSE = 5
SESSION_STATUS = 2
SUBSCRIPTION_DATA = 8
SUBSCRIPTION_STATUS = 3
TIMEOUT = 10
TOKEN_STATUS = 15
TOPIC_STATUS = 14
UNKNOWN = -1
BLPAPI_DATATYPE_BOOL = 1
BLPAPI_DATATYPE_CHAR = 2
BLPAPI_DATATYPE_BYTE = 3
BLPAPI_DATATYPE_INT32 = 4
BLPAPI_DATATYPE_INT64 = 5
BLPAPI_DATATYPE_FLOAT32 = 6
BLPAPI_DATATYPE_FLOAT64 = 7
BLPAPI_DATATYPE_STRING = 8
BLPAPI_DATATYPE_BYTEARRAY = 9
BLPAPI_DATATYPE_DATE = 10
BLPAPI_DATATYPE_TIME = 11
BLPAPI_DATATYPE_DECIMAL = 12
BLPAPI_DATATYPE_DATETIME = 13
BLPAPI_DATATYPE_ENUMERATION = 14
BLPAPI_DATATYPE_SEQUENCE = 15
BLPAPI_DATATYPE_CHOICE = 16
BLPAPI_DATATYPE_CORRELATION_ID = 17


class blpQuery:
    session = []
    service = []

    def __init__(self):
        """class to handle reference and historical bloomberg queries"""
        self.session = win32com.client.Dispatch('blpapicom.Session')
        self.session.QueueEvents = True
        self.session.Start()
        self.session.OpenService('//blp/refdata')
        self.service = self.session.GetService('//blp/refdata')

    def getdata(self, securities, fields, parameters=[], overrides=[]):
        """reference query, returns a table, with query time
        and queried data"""

        retDic = {}
        request = self.service.CreateRequest('ReferenceDataRequest')

        for i in range(len(securities)):
            request.GetElement('securities').AppendValue(securities[i])
        for i in range(len(fields)):
            request.GetElement('fields').AppendValue(fields[i])
        for i in range(len(parameters)):
            request.Set(parameters[i][0], parameters[i][1])
        for i in range(len(overrides)):
            override1 = request.GetElement('overrides').AppendElment()
            override1.SetElement("fieldId", overrides[i][0])
            override1.SetElement("value", overrides[i][1])

        self.session.SendRequest(request)
        while True:
            event = self.session.NextEvent()
            iterator = event.CreateMessageIterator()
            while iterator.Next():
                message = iterator.Message
                if (event.EventType == PARTIAL_RESPONSE or
                        event.EventType == RESPONSE):
                    securityData = message.GetElement('securityData')
                    for i in range(len(securities)):
                        returnList = []
                        security = securityData.GetValueAsElement(i)
                        securityName = security.GetElement('security')
                        fieldData = security.GetElement('fieldData')
                        c = 0
                        for col in range(len(fields)):
                            if fieldData.HasElement(fields[col]):
                                colField = fieldData.GetElement(c)
                                c += 1
                                d_type = colField.datatype
                                if d_type == BLPAPI_DATATYPE_SEQUENCE:
                                    numBk = colField.numValues
                                    res = []
                                    for bki in range(numBk):
                                        elem = colField.GetValueAsElement(bki)
                                        numEl = elem.numElements
                                        res_elem = {}
                                        for bke in range(numEl):
                                            ee = elem.GetElement(bke)
                                            v = ee.value
                                            # patch for pytime
                                            if str(type(v)) == "<type 'time'>":
                                                v = datetime.datetime.\
                                                    fromtimestamp(int(v))
                                            res_elem[ee.name] = v
                                        res.append(res_elem)
                                    returnList.append(res)

                                else:
                                    returnList.append(colField.value)
                            else:
                                returnList.append(float('nan'))
                        retDic[str(securityName)] = \
                            [datetime.datetime.now()] + returnList

            if event.EventType == RESPONSE:
                break
        return retDic

    def history(self, securities, fields, dates, parameters=[], overrides=[]):
        """historical query, returns a table with time and field value"""
        retDic = {}
        request = self.service.CreateRequest('HistoricalDataRequest')

        for i in range(len(securities)):
            request.GetElement('securities').AppendValue(securities[i])
        for i in range(len(fields)):
            request.GetElement('fields').AppendValue(fields[i])
        for i in range(len(parameters)):
            request.Set(parameters[i][0], parameters[i][1])
        for i in range(len(overrides)):
            override1 = request.GetElement('overrides').AppendElment()
            override1.SetElement("fieldId", overrides[i][0])
            override1.SetElement("value", overrides[i][1])

        request.Set('startDate', dates[0])
        request.Set('endDate', dates[1])

        self.session.SendRequest(request)
        while True:
            event = self.session.NextEvent()
            if (event.EventType == PARTIAL_RESPONSE or
                    event.EventType == RESPONSE):

                iterator = event.CreateMessageIterator()
                while iterator.Next():
                    message = iterator.Message
                    securityData = message.GetElement('securityData')
                    securityName = securityData.GetElement('security')
                    fieldData = securityData.GetElement('fieldData')
                    returnList = []
                    for row in range(fieldData.NumValues):
                        rowField = fieldData.GetValue(row)
                        row = []
                        c = 1
                        for col in range(len(fields) + 1):
                            if col == 0:
                                tt = datetime.datetime.fromtimestamp(int(
                                    rowField.GetElement(col).value))
                                row.append(tt)
                            if col != 0:
                                if rowField.HasElement(fields[col - 1]):
                                    colField = rowField.GetElement(c)
                                    c += 1
                                    row.append(colField.value)
                                else:
                                    row.append(float('nan'))
                        returnList.append(row)
                    retDic[str(securityName)] = returnList

                if event.EventType == RESPONSE:
                    break
        return retDic


def output_table(table):
    """output returned reference and historical table in a formatted way"""
    for i, j in table.iteritems():
        print '{0}: \n'.format(i)
        for k in range(len(j)):
            if isinstance(j[k], list):
                for kk in range(len(j[k])):
                    if isinstance(j[k][kk], dict):
                        sys.stdout.write('\t%s\n' % j[k][kk])
                    else:
                        sys.stdout.write('\t%s |' % j[k][kk])
            else:
                sys.stdout.write('\t%s |' % j[k])
        sys.stdout.write('\n')


def join_table(table1, table2):
    """join two tables of the same type. Tables must share identical
    securities and time spans"""

    for i, j in table1.iteritems():
        j1 = table2[i]
        islist = 0
        for k in range(len(j)):
            if islist or isinstance(j[k], list):
                table1[i][k] = j[k] + j1[k][1:]
                islist = 1
        if islist == 0:
            table1[i] = j + j1[1:]
    return table1


if __name__ == '__main__':
    # for testing, the code should never be called directely
    blp = blpQuery()
    t1 = blp.getdata(['1 HK Equity'], ['TOT_ANALYST_REC'])
    t2 = blp.getdata(['1 HK Equity'],
                     ['SHORT_NAME', 'GICS_SECTOR_NAME',
                      'GICS_INDUSTRY_NAME', 'GICS_SUB_INDUSTRY_NAME'])
    output_table(join_table(t1, t2))
    t1 = blp.history(['1 HK Equity', '5 HK Equity'], ['TOT_ANALYST_REC'],
                     ['20110101', '20110110'])
    t1 = join_table(t1, blp.history(['1 HK Equity', '5 HK Equity'],
                                    ['BEST_EPS'], ['20110101', '20110110'],
                                    [['currency', 'HKD']],
                                    [['BEST_FPERIOD_OVERRIDE', 'BF']]))
    t1 = join_table(t1, blp.history(['1 HK Equity', '5 HK Equity'],
                                    ['PX_LAST'],
                                    ['20110101', '20110110'],
                                    [['currency', 'HKD']]))
    t1 = join_table(t1, blp.history(['1 HK Equity', '5 HK Equity'],
                                    ['PX_TO_BOOK_RATIO', 'VOLUME',
                                     'BETA_RAW_OVERRIDABLE', 'PX_HIGH',
                                     'PX_LOW'],
                                    ['20110101', '20110110']))
    output_table(t1)
