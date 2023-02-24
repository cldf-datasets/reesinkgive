from collections import OrderedDict
import csv
import pathlib

from cldfbench import CLDFSpec, Dataset as BaseDataset


def read_data(stream):
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


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "reesinkgive"

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
        self.raw_dir.xlsx2csv('Reesink2013_modified.xlsx')

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
        data_file = self.raw_dir / 'Reesink2013_modified.Sheet1.csv'
        with open(data_file, encoding='utf-8') as f:
            raw_data = read_data(f)
        # glottolog = args.glottolog
