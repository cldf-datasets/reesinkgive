from collections import OrderedDict
import csv
from itertools import groupby
import pathlib
import re

from cldfbench import CLDFSpec, Dataset as BaseDataset
from pybtex.database import parse_file


def read_data(stream):
    # TODO: remove?
    # this manually deals with the csv files converted from excel
    csv_reader = csv.reader(stream)
    # drop header (and check if things have changed)
    assert next(csv_reader) == [
        'Language name', 'Glottocode', 'Order with theme suffix',
        'Order with theme suffix', 'Order with no theme affix', 'Recipient vs.',
        'Reference', 'Page numbers', 'Glotto ref ID']
    assert next(csv_reader) == ['', '', '', '', '', 'theme', '', '', '']
    data = OrderedDict()
    current_language = None
    for (
        _, glottocode, suffix_1, suffix_2, no_theme, recipient_vs_theme,
        citations, pages, glottolog_ref
    ) in csv_reader:
        if glottocode:
            current_language = glottocode
            data[current_language] = OrderedDict()
        if suffix_1:
            data[current_language]['order-with-suffix-1'] = suffix_1
        if suffix_2:
            data[current_language]['order-with-suffix-2'] = suffix_2
        if no_theme:
            data[current_language]['order-without-affix'] = no_theme
        if recipient_vs_theme:
            data[current_language]['recipient-vs-theme'] = recipient_vs_theme
        if citations:
            if 'citations' not in data[current_language]:
                data[current_language]['citations'] = []
            data[current_language]['citations'].append(citations)
        if pages:
            if 'pages' not in data[current_language]:
                data[current_language]['pages'] = []
            data[current_language]['pages'].append(pages)
        if glottolog_ref:
            if 'glottolog_refs' not in data[current_language]:
                data[current_language]['glottolog_refs'] = []
            data[current_language]['glottolog_refs'].append(glottolog_ref)
    return data


def normalise_header(header):
    return re.sub(r'\s+', ' ', header.strip())


def normalise_cell(cell):
    return cell.strip()


def languoid_to_lang(languoid, language_names, source_assocs):
    language = {
        'ID': languoid.glottocode,
        'Name': language_names.get(languoid.glottocode) or languoid.name,
        'Glottocode': languoid.glottocode,
    }
    isocode = languoid.iso
    if isocode:
        language['ISO639P3code'] = isocode
    if languoid.latitude:
        language['Latitude'] = languoid.latitude
        language['Longitude'] = languoid.longitude
    macroareas = languoid.macroareas
    if macroareas:
        language['Macroarea'] = macroareas[0].name
    if (sources := source_assocs.get(languoid.glottocode)):
        language['Source'] = list(map(format_source, sources))
        language['Source_comment'] = '; '.join(
            trimmed
            for source in sources
            if (trimmed := source.get('prose_ref', '').strip()))
    return language


def format_source(source_assoc):
    if (pages := source_assoc.get('pages')):
        return '{}[{}]'.format(
            source_assoc['glottolog_ref'],
            pages.replace(';', ','))
    else:
        return source_assoc['glottolog_ref']


def make_code_id(parameter_id, value):
    return '{}-{}'.format(
        parameter_id,
        re.sub(r'[^a-z0-9\-_]', '-', value.lower()))


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = 'reesinkgive'

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module="StructureDataset",
            metadata_fname='cldf-metadata.json')

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        # we switched over to using csv files directly
        # self.raw_dir.xlsx2csv('Reesink2013_modified.xlsx')

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
        with open(self.raw_dir / 'Reesink2013.csv', encoding='utf-8') as f:
            rdr = csv.reader(f, delimiter=';')
            header = list(map(normalise_header, next(rdr)))
            raw_data = [
                dict(zip(header, map(normalise_cell, row)))
                for row in rdr]

        former_excel_sheet = self.raw_dir / 'Reesink2013_modified.Sheet1.csv'
        with open(former_excel_sheet, encoding='utf-8') as f:
            rdr = csv.reader(f)
            header = next(rdr)
            assert (header[0], header[1]) == ('Language name', 'Glottocode')
            # drop that second header line
            _ = next(rdr)
            language_names = {
                glottocode: language_name
                for row in rdr
                if (language_name := row[0].strip())
                and (glottocode := row[1].strip())}

        with open(self.etc_dir / 'sources.bib', encoding='utf-8') as f:
            sources = parse_file(f, 'bibtex')
        source_assocs = {
            glottocode: list(rows)
            for glottocode, rows in groupby(
                self.etc_dir.read_csv('source_assocs.csv', dicts=True),
                lambda r: r['glottocode'])}

        parameters = {
            param['Original_Name']: param
            for param in self.etc_dir.read_csv('parameters.csv', dicts=True)}

        glottolog = args.glottolog.api
        glottocodes = {row['Glottocode'] for row in raw_data}
        language_table = [
            languoid_to_lang(languoid, language_names, source_assocs)
            for languoid in glottolog.languoids(ids=glottocodes)]

        value_table = [
            {
                'ID': '{}-{}'.format(row['Glottocode'], parameter['ID']),
                'Language_ID': row['Glottocode'],
                'Parameter_ID': parameter['ID'],
                'Code_ID': make_code_id(parameter['ID'], value),
                'Value': value,
            }
            for row in raw_data
            for column_name, value in row.items()
            if (parameter := parameters.get(column_name))]

        codes = {}
        for value in value_table:
            code_id = make_code_id(value['Parameter_ID'], value['Value'])
            if code_id in codes:
                continue
            codes[code_id] = {
                'ID': code_id,
                'Parameter_ID': value['Parameter_ID'],
                'Name': value['Value'],
            }
        code_table = sorted(codes.values(), key=lambda c: c['ID'])

        args.writer.cldf.add_component(
            'LanguageTable',
            'http://cldf.clld.org/v1.0/terms.rdf#source',
            'Source_comment')
        args.writer.cldf.add_component('ParameterTable')
        args.writer.cldf.add_component('CodeTable')

        args.writer.objects['LanguageTable'] = language_table
        args.writer.objects['ParameterTable'] = parameters.values()
        args.writer.objects['CodeTable'] = code_table
        args.writer.objects['ValueTable'] = value_table
        args.writer.cldf.add_sources(sources)
