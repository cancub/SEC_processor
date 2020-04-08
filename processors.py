from copy import deepcopy
from datetime import datetime
from xml.etree import ElementTree as ET

import utils

class EdgarException(Exception):
    pass

class Edgar(object):
    def __init__(self, url = None):
        self.url = None
        self.report_date = None
        self.doc_type = None
        self.name = None
        self.characteristics = {
            'isDirector': False,
            'isOfficer': False,
            'isTenPercentOwner': False,
            'isOther': False,
        }
        self.holdings = []
        self.transactions = []

        if url is not None:
            self.get_and_parse_xml(url)

    def get_and_parse_xml(self, url):
        if not isinstance(url, str):
            raise TypeError(
                'Excpecting string for url, got {}'.format(
                    url.__class__.__name__))

        # This will raise an exception if the url is bad or we get a non-200 code
        r = utils.get_and_check_URL(url)

        data = ET.fromstring(r.content)
        if data.tag != 'ownershipDocument':
            raise EdgarException('No Edgar stock information found')

        self.url = url

        self.doc_type = data.find('documentType').text

        self.report_date = datetime.strptime(
            data.find('periodOfReport').text, '%Y-%m-%d')
        
        # Get information about the owner
        owner = data.find('reportingOwner')
        self.name = owner.find('reportingOwnerId/rptOwnerName').text
        owner_types = owner.find('reportingOwnerRelationship')
        for c in self.characteristics:
            owner_char = owner_types.find(c)
            if owner_char is not None:
                self.characteristics[c] = owner_types.text == '1'

        # Get information about the stocks
        for t_elem in data.findall('nonDerivativeTable/nonDerivativeTransaction'):
            self.transactions.append(Transaction(t_elem))
        for h_elem in data.findall('nonDerivativeTable/nonDerivativeHolding'):
            self.holdings.append(Holding(h_elem))

    def build_updates_list(self):
        result = []
        # Get the total of all of the ownership types that did not change in this
        # document (i.e., they are there for reference)
        static_holdings_total = sum((x.current_quantity for x in self.holdings))

        # We'll copy this to generate each of the daily counts
        base_holdings_count = {
            'owner': self.name,
            'url': self.url,
            'total': static_holdings_total,
        }
        base_holdings_count.update(self.characteristics)

        if self.doc_type == '3':
            # We know that this is an initial filing for a company insider, so
            # we can just return the above base_holdings_count
            form_3_info = deepcopy(base_holdings_count)
            form_3_info['date'] = self.report_date
            result = [form_3_info]

        elif self.doc_type == '4':
            daily_end_counts = {}

            # Walk through each transaction to determine the final count for the
            # specific ownership type at the end of each date in the document
            for t in self.transactions:
                if t.date not in daily_end_counts:
                    daily_end_counts[t.date] = {}
                # We can forget about the previous count for this ownership type
                # because this is the most recent value (based on order)
                daily_end_counts[t.date][t.ownership] = t.current_quantity

            # Now we can add the totals for each day for transactions plus static
            # holdings
            for date in sorted(daily_end_counts.keys()):
                new_holdings_count = deepcopy(base_holdings_count)
                new_holdings_count['date'] = date
                for current_quantity in daily_end_counts[date].values():
                    new_holdings_count['total'] += current_quantity
                result.append(new_holdings_count)

        elif self.doc_type == '4/A':
            # TODO: deal with whatever the hell this is
            pass

        return result

class Holding(object):
    def __init__(self, xml_node = None):
        self.security_type = None
        self.current_quantity = None
        self.ownership = None

        if xml_node is not None:
            self.parse_holding(xml_node)

    def parse_holding(self, xml_node):
        if not isinstance(xml_node, ET.Element):
            raise TypeError(
                'Cannot work with objects of type {}'.format(
                    xml_node.__class__.__name__))
        if xml_node.tag not in ('nonDerivativeHolding', 'nonDerivativeTransaction'):
            raise TypeError('Cannot work with {} elements'.format(xml_node.tag))

        self.security_type = xml_node.find('securityTitle/value').text
        self.current_quantity = float(xml_node.find(
            'postTransactionAmounts/sharesOwnedFollowingTransaction/value').text)
        nature = xml_node.find('ownershipNature')
        self.ownership = nature.find('directOrIndirectOwnership/value').text.upper()
        if self.ownership != 'D':
            self.ownership += ': {}'.format(
                nature.find('natureOfOwnership/value').text)

class Transaction(Holding):
    def __init__(self, xml_node = None):
        super().__init__(xml_node)
        self.date = None
        self.amount = None
        self.price = None
        self.transaction_type = None

        if xml_node is not None:
            self.parse_transaction(xml_node)

    def parse_transaction(self, xml_node):
        if not isinstance(xml_node, ET.Element):
            raise TypeError(
                'Cannot work with objects of type {}'.format(
                    xml_node.__class__.__name__))
        if xml_node.tag != 'nonDerivativeTransaction':
            raise TypeError('Cannot work with {} elements'.format(xml_node.tag))

        self.date = datetime.strptime(
            xml_node.find('transactionDate/value').text, '%Y-%m-%d')
        amounts = xml_node.find('transactionAmounts')
        self.amount = float(amounts.find('transactionShares/value').text)
        price = amounts.find('transactionPricePerShare/value')
        if price is not None:
            # price is nice to have, but not necessary
            self.price = float(price.text)
        self.transaction_type = amounts.find(
            'transactionAcquiredDisposedCode/value').text