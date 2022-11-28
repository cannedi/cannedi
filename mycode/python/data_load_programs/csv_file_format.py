
class CSVFileFormat(object):
    def __init__(self):
        self.file_format='csv'
        self.quote=None
        self.delimiter=','
        self.has_header=True
        self.file_encoding='UTF8'
        self.null_string=None
        self.escape_char=None

    def __str__(self):
        return ''.join(f'File Format: {self.file_format}') \
                    .join(f'\n Quote: {self.quote}') \
                    .join(f'\n Delimiter: {self.delimiter}') \
                    .join(f'\n Has header: {self.has_header}') \
                    .join(f'\n File Encoding: {self.file_encoding}') \
                    .join(f'\n Null string: {self.null_string}') \
                    .join(f'\n Escape char: {self.escape_char}')

    def is_format_valid(self):
        return self.valid_file_format() \
            and self.valid_quote() \
            and self.valid_delimiter() \
            and self.valid_encoding() \
            and self.valid_null_string() \
            and self.valid_escape_char()

    def valid_file_format(self):
        if self.file_format in ['csv', 'txt', 'CSV', 'TXT']:
            return True
        return False
    def valid_quote(self):
        if self.quote == None or self.quote in ['"', '\'']:
            return True
        return False
    def valid_delimiter(self):
        if self.delimiter != None and self.delimiter in [',', '\\t', ':']:
            return True
        return False
    def valid_encoding(self):
        if self.file_encoding != None and len(self.file_encoding) > 0 \
            and self.file_encoding in ['UTF8','LATIN1']:
            return True
        return False
    def valid_null_string(self):
        if self.null_string == None or self.null_string in ['null', 'NULL', '']:
            return True
        return False
    def valid_escape_char(self):
        if self.escape_char == None or self.escape_char in ['"', '\\']:
            return True
        return False

    def postgres_with_options(self):
        header = ',HEADER' if self.has_header else ''
        nullstr = f',NULL \'\'{self.null_string}\'\'' if self.null_string != None else ''
        escapestr = f',ESCAPE \'\'{self.escape_char}\'\'' if self.escape_char != None else ''
        quotestr = f',QUOTE \'\'{self.quote}\'\'' if self.quote != None else ''
        return f'( FORMAT {self.file_format},DELIMITER \'\'{self.delimiter}\'\'{nullstr}{header}{quotestr}{escapestr},ENCODING \'\'{self.file_encoding}\'\')'
