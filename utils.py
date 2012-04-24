class TablePrint:
    """
    Print table on the CLI.
    """

    def __init__(self, table):
        """
        Table is [(1,2,3,), (4,5,6,),] etc data.
        """
        self.table = table
        self.widths = []


    def _check(self):
        """
        Check if table is consistent grid.
        Header is a leader here.
        """
        if not len(self.table):
            raise Exception("Table is empty!")

        header = None
        for row in self.table:
            if header == None:
                header = len(row)
                continue
            if len(row) != header:
                raise Exception("Table has different row widths.")


    def _get_widths(self):
        """
        Find extra-widths by max width of any value.
        """

        self.widths = [0 for x in self.table[0]]
        for row in self.table:
            for idx in range(len(row)):
                cell_len = len(str(row[idx]))
                if cell_len > self.widths[idx]:
                    self.widths[idx] = cell_len


    def _format(self):
        """
        Format the output.
        """
        out = []
        ftable = []
        for row in self.table:
            frow = []
            for idx in range(len(row)):
                frow.append(str(row[idx]) + (" " * (self.widths[idx] - len(str(row[idx])))))
            ftable.append(frow)

        for idx in range(len(ftable)):
            out.append(' | '.join(ftable[idx]))
            if idx == 0:
                out.append('-+-'.join(["-" * len(item) for item in ftable[idx]]))

        return '\n'.join(out)


    def __str__(self):
        self._check()
        self._get_widths()
        return self._format()



if __name__ == '__main__':
    table = [
        ('Tablespace', 'Size (Mb)', 'Avail (Mb)', 'Use %'),
        ('template1', 6, 94564, '0.5'),
        ('susemanager', 6, 945646, '3.4'),
        ('data_fs', 8, 345644, '0.5'),
        ('something', 7, 84542, '1.9'),
        ]

    print TablePrint(table)
