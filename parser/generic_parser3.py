#!/usr/bin/python3

# A generic XML to SQL query parser
import lxml.etree as etree
import os
import sys
import datetime
import csv
from string import Template
from optparse import OptionParser

# Set up the dictionaries as global so we're not endlessly passing them back and forth. We write these in the ReadConfig process, then just read from there on in.
table_dict = {}
value_dict = {}
ctr_dict = {}
attrib_dict = {}
attrib_defaults = {}
file_number_dict = {}

# Set these globally, but may need to change them via options between MySQL and PostgreSQL
table_quote = '"'
value_quote = "'"


def main():
    # STEP 1 - process the options

    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="filename", help="parse a single file")
    parser.add_option("-d", "--directory", dest="directory", help="a directory in which all XML files will be parsed.")
    parser.add_option("-c", "--config", dest="config_file", help="configuration file")
    parser.add_option("-t", "--template", dest="template_file", help="template file")
    parser.add_option("-o", "--output", dest="output", help="output file or directory")
    parser.add_option("-p", "--parent", dest="parent",
                      help="Name of the parent tag (tag containing the group of records)")
    parser.add_option("-r", "--record", dest="record", help="Name of the tag that defines a single record")
    parser.add_option("-n", "--namespace", dest="namespace", help="Namespace of the XML file")
    parser.add_option("-i", "--identifier", dest="identifier",
                      help="Name of the tag whose value contains the unique identifier for the record")
    parser.add_option("-l", "--file_number", dest="file_number_sheet",
                      help="CSV file with the file name to file number lookup")
    parser.add_option("-m", "--database_mode", dest="database_mode", help="MySQL or Postgres, defaults to Postgres")
    parser.add_option("-s", "--single_trans", dest="single_trans", help="If true, will enable one transaction per file")
    parser.add_option("-z", "--recurse", dest="recurse",
                      help="If true and a directory is set, the parser will search subdirectories for XML files")

    (options, args) = parser.parse_args()

    # Read the configuration
    template_file = options.template_file
    config_file = options.config_file
    single_trans = options.single_trans if options.single_trans else False

    if options.filename is None and options.directory is None:
        print("ERROR: No target to parse")
        sys.exit(1)

    if options.filename is not None and options.directory is not None:
        print("ERROR: Both target file and directory specified")
        sys.exit(1)

    if options.filename:
        mode = "file"
        filelist = [options.filename]
        output_dir = options.output if os.path.isdir(options.output) else None
        output_file = None if os.path.isdir(options.output) else options.output

    if options.directory:
        mode = "directory"
        filelist = getXmlFiles(options.directory, options.recurse)
        output_dir = options.output if not os.path.isfile(options.output) else None

    global table_quote
    db_mode = "mysql" if str(options.database_mode).lower() == "mysql" else "postgres"
    table_quote = "`" if db_mode == "mysql" else '"'

    namespace = options.namespace if options.namespace else ''
    root_tag = options.parent
    rec_tag = options.record
    id_tag = options.identifier

    file_number_lookup = {}
    if options.file_number_sheet:
        print("File numbers found")
        with open(options.file_number_sheet, mode='r') as infile:
            reader = csv.reader(infile)
            file_number_lookup = dict((rows[0], rows[1]) for rows in reader)

    # STEP 2 - Convert the config file into lookup tables
    root = etree.parse(open(config_file)).getroot()
    ReadConfig(root, "", namespace)

    # get the template from the file
    tfile = open(template_file)
    template = Template(tfile.read())

    # STEP 3 - Parse the file(s)
    for filename in filelist:
        outputtarget = os.path.join(output_dir, os.path.split(filename)[1][
                                                :-4] + "-queries.sql") if mode == "directory" else output_file
        try:
            with open(filename):
                pass
        except IOError:
            print(f"Error opening file: {filename}")
            continue

        with open(outputtarget, "w") as output:
            if db_mode == "mysql":
                output.write("SET unique_checks=0;\nSET autocommit=0;\n")
            if single_trans:
                output.write("BEGIN;\n")

            shortfilename = os.path.split(filename)[1]
            file_number = file_number_lookup.get(shortfilename, -1)

            print(f"Parsing file: {filename}")
            print(f"Start time: {datetime.datetime.now()}")
            process = True if not root_tag else False

            try:
                parser = etree.iterparse(filename, remove_comments=True, recover=True, events=("start", "end"))
            except:
                parser = etree.iterparse(filename, remove_comments=True, events=("start", "end"))

            for event, elem in parser:
                if event == "end" and process and elem.tag == f"{namespace}{rec_tag}":
                    tableList = TableList()
                    statementList = []

                    table_path = f"{namespace}{root_tag}/{rec_tag}" if root_tag else f"{namespace}{rec_tag}"
                    core_table_name = table_dict[table_path]

                    tableList.AddTable(core_table_name, None, table_path)

                    id_value = f"'{elem.find(f'{namespace}{id_tag}').text}'" if id_tag != rec_tag else f"'{elem.text}'"
                    tableList.AddIdentifier(core_table_name, 'id', id_value)

                    if table_path in file_number_dict:
                        file_number_name = file_number_dict[table_path]
                        tableList.AddCol(file_number_name.split(":", 1)[0], file_number_name.split(":", 1)[1],
                                         file_number)

                    attribSeen = set()
                    for attribName, attribValue in elem.attrib.items():
                        attribpath = f"{table_path}/{attribName}"
                        if attribpath in attrib_dict:
                            tableName, colName = attrib_dict[attribpath].split(":")[:2]
                            tableList.AddCol(tableName, colName, str(attribValue))
                            attribSeen.add(attribName)

                    for attribName, attribValueAll in attrib_defaults.get(table_path, {}).items():
                        if attribName not in attribSeen:
                            tableName, colName, attribValue = attribValueAll.split(":")[:3]
                            tableList.AddCol(tableName, colName, str(attribValue))

                    if elem.text and table_path in value_dict:
                        tableList.AddCol(value_dict[table_path].split(":")[0], value_dict[table_path].split(":")[1],
                                         str(elem.text))

                    for child in elem:
                        ParseNode(child, table_path, tableList, core_table_name, statementList)

                    tableList.CloseTable(core_table_name, statementList)

                    data = "".join(str(statement) + "\n" for statement in reversed(statementList))

                    template_dict = {'data': data, 'file_number': file_number, 'id': id_value}
                    final = template.substitute(template_dict)
                    output.write(final)

                    output.flush()
                    elem.clear()

            if db_mode == "mysql":
                output.write("SET unique_checks=1;\nSET autocommit=1;\n")
            if single_trans:
                output.write("COMMIT;\n")

        print(f"End time: {datetime.datetime.now()}")


def ParseNode(node, path, tableList, last_opened, statementList):
    if node.tag.find("}") > -1:
        tag = node.tag.split("}", 1)[1]
    else:
        tag = node.tag
    newpath = f"{path}/{tag}"

    table_path = f"{newpath}/table"
    if table_path in table_dict:
        new_table = True
        table_name = table_dict[table_path]
        tableList.AddTable(table_name, last_opened, newpath)
    else:
        new_table = False
        table_name = last_opened

    if newpath in file_number_dict:
        file_number_name = file_number_dict[newpath]
        tableList.AddCol(file_number_name.split(":", 1)[0], file_number_name.split(":", 1)[1], file_number)

    attribSeen = set()
    for attribName, attribValue in node.attrib.items():
        attribpath = f"{newpath}/{attribName}"
        if attribpath in attrib_dict:
            tableName, colName = attrib_dict[attribpath].split(":")[:2]
            tableList.AddCol(tableName, colName, str(attribValue))
            attribSeen.add(attribName)

    # Process default attribute values
    for attribName, attribValueAll in attrib_defaults.get(newpath, {}).items():
        if attribName not in attribSeen:
            tableName, colName, attribValue = attribValueAll.split(":")[:3]
            tableList.AddCol(tableName, colName, str(attribValue))

        # Process value
        if newpath in value_dict:
            if node.text:
                tableList.AddCol(value_dict[newpath].split(":")[0], value_dict[newpath].split(":")[1], str(node.text))

        # Process children
        for child in node:
            ParseNode(child, newpath, tableList, table_name, statementList)

        if new_table:
            tableList.CloseTable(table_name, statementList)


def ReadConfig(node, path, namespace):
    newpath = path + node.tag + "/"

    if node.text:
        if str(node.text).strip():
            value_dict["%s%s" % (namespace, newpath)] = node.text

    for attribName, attribValueAll in node.attrib.items():
        attribValue = ':'.join(attribValueAll.split(':')[:2])
        attrib_path = newpath + attribName
        if attribName == "table":
            table_dict["%s%s" % (namespace, attrib_path)] = attribValue
        elif attribName == "ctr_id":
            ctr_dict["%s%s" % (namespace, attrib_path)] = attribValue
        elif attribName == "file_number":
            file_number_dict["%s%s" % (namespace, attrib_path)] = attribValue
        else:
            attrib_dict["%s%s" % (namespace, attrib_path)] = attribValue
            if len(attribValueAll.split(':')) == 3:
                defaults = attrib_defaults.setdefault(("%s%s" % (namespace, newpath)).strip('/'), {})
                defaults[attribName] = attribValueAll

    for child in node:
        ReadConfig(child, newpath, namespace)


class TableList:
    def __init__(self):
        self.tlist = []

    def AddTable(self, tableName, parentName, tablePath):
        t = Table(tableName, parentName, self, tablePath)
        self.tlist.append(t)

    def AddCol(self, tableName, colName, colValue):
        for t in self.tlist:
            if t.name == tableName:
                t.AddCol(colName, colValue)

    def AddIdentifier(self, tableName, colName, colValue):
        for t in self.tlist:
            if t.name == tableName:
                t.AddIdentifier(colName, colValue)

    def CloseTable(self, tableName, statementList):
        for t in self.tlist:
            if t.name == tableName:
                statementList.append(t.createInsert())
                self.tlist.remove(t)
                del t


class Table:
    def __init__(self, name, parent_name, table_list, tablePath):
        self.name = name
        self.columns = []
        self.identifiers = []
        self.counters = []
        self.parent_name = parent_name
        if parent_name:
            for table in table_list.tlist:
                if table.name == parent_name:
                    parent = table
                    for identifier in parent.GetIdentifiers():
                        self.AddIdentifier(identifier.name, identifier.value)
                    new_id = parent.GetCounter(tablePath)
                    self.AddIdentifier(new_id.name, new_id.value)

    def AddCol(self, colName, colValue):
        newcol = Column(colName, colValue)
        self.columns.append(newcol)

    def AddIdentifier(self, colName, colValue):
        newcol = Column(colName, colValue)
        self.identifiers.append(newcol)

    def GetCounter(self, name):
        ctr_id = ctr_dict[name + "/ctr_id"].split(":", 1)[1]
        for counter in self.counters:
            if counter.name == ctr_id:
                counter.value += 1
                return counter
        newcounter = Column(ctr_id, 1)
        self.counters.append(newcounter)
        return newcounter

    def GetIdentifiers(self):
        return self.identifiers

    def createInsert(self):
        colList = ""
        valList = ""
        for col in self.identifiers:
            if colList:
                colList += ","
                valList += ","
            colList += f"{table_quote}{col.name}{table_quote}"
            valList += str(col.value)
        for col in self.columns:
            if colList:
                colList += ","
                valList += ","
            colList += f"{table_quote}{col.name}{table_quote}"
            valList += f"{value_quote}{db_string(col.value)}{value_quote}"
        statement = f"INSERT INTO {table_quote}{self.name}{table_quote} ({colList}) VALUES ({valList});"
        return statement


class Column:
    def __init__(self, name, value):
        self.name = name
        self.value = value


def db_string(s):
    if s is not None:
        return str(s).replace("'", "''").replace('\\', '\\\\')
    else:
        return "NULL"


def getXmlFiles(directory, recurse):
    filelist = []
    if recurse:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.xml'):
                    filelist.append(os.path.join(root, file))
    else:
        for filename in sorted(os.listdir(directory)):
            if filename.endswith('.xml'):
                filelist.append(os.path.join(directory, filename))
    filelist.sort()
    return filelist


if __name__ == "__main__":
    sys.exit(main())
