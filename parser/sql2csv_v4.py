import csv
import os
import re
from collections import defaultdict

def parse_values(value_string):
    values, temp, in_quotes = [], '', False
    quote_char = None  # Track which quote is used (single or double)

    for char in value_string:
        if char in ["'", '"'] and not in_quotes:
            in_quotes = True
            quote_char = char
        elif char == quote_char and in_quotes:
            in_quotes = False
            quote_char = None

        if char == ',' and not in_quotes:
            values.append(temp.strip())
            temp = ''
        else:
            temp += char

    if temp:  # Add the last value
        values.append(temp.strip())

    return values


def align_columns_and_values(columns, values, schema):
    # Ensure that the output has the right length by matching the schema
    row_data = {col: '' for col in schema}  # Initialize all fields to empty string

    # Loop through the columns and align with values
    for col, val in zip(columns, values):
        if col in schema:
            row_data[col] = val

    # If there are more columns in schema than values, leave them as empty strings
    return row_data


def extract_data_from_sql(file_content, error_log_path, mismatch_log_path):
    pattern = re.compile(r"INSERT INTO (.+?) \((.+?)\) VALUES \((.+?)\);", re.DOTALL)
    data_by_table = defaultdict(list)

    with open(error_log_path, 'a') as error_file, open(mismatch_log_path, 'a') as mismatch_file:
        lines = file_content.split('\n')
        combined_line = ""
        in_quotes = False
        quote_char = None

        for line in lines:
            stripped_line = line.strip()

            # Skip empty lines
            if not stripped_line:
                continue

            # Process each character to detect if we're inside a quoted string
            for char in stripped_line:
                if char in ["'", '"']:
                    if in_quotes and char == quote_char:
                        in_quotes = False  # End of quoted string
                    elif not in_quotes:
                        in_quotes = True
                        quote_char = char  # Start of a quoted string
                elif char == ';' and not in_quotes:
                    # Found the end of the query (semicolon outside quotes)
                    combined_line += char
                    break
                combined_line += char

            # Check if the combined line has a full query (semicolon outside quotes)
            if combined_line.endswith(';'):
                match = pattern.match(combined_line.strip())
                if match:
                    table_name, column_part, value_part = match.groups()
                    columns = [col.strip() for col in column_part.split(',')]
                    values = parse_values(value_part)

                    # Attempt to handle mismatches: Align the columns and values based on the schema
                    if len(columns) != len(values):
                        # Log the mismatch, but attempt to align columns with values
                        error_file.write(f"Mismatch in table {table_name}: {combined_line}\n")
                        mismatch_file.write(combined_line + '\n')  # Save mismatched query
                        schema = table_schemas.get(table_name, [])
                        row_data = align_columns_and_values(columns, values, schema)
                        data_by_table[table_name].append(row_data)
                    else:
                        data_by_table[table_name].append(dict(zip(columns, values)))
                else:
                    print(f"No match for line: {combined_line.strip()}")  # Debugging

                # Reset the combined line
                combined_line = ""
            else:
                combined_line += " "

    return data_by_table


def append_to_csv(data_by_table, csv_directory, table_schemas):
    for table_name, rows in data_by_table.items():
        if table_name not in table_schemas:
            continue
        schema = table_schemas[table_name]
        csv_file_path = os.path.join(csv_directory, "{}.csv".format(table_name))
        file_exists = os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=schema)
            if not file_exists:
                writer.writeheader()
            for row in rows:
                row_data = {field: row.get(field, '') for field in schema}
                writer.writerow(row_data)


def process_sql_files(sql_directory, csv_directory, table_schemas, error_log_path, mismatch_log_path):
    for sql_filename in os.listdir(sql_directory):
        if sql_filename.endswith(".sql"):
            with open(os.path.join(sql_directory, sql_filename), 'r') as file:
                file_content = file.read()
                data_by_table = extract_data_from_sql(file_content, error_log_path, mismatch_log_path)
                append_to_csv(data_by_table, csv_directory, table_schemas)

# Define schemas and paths as before
table_schemas = {
    'clarivate-datapipline-project.bq_wos_2024_data.wos_abstract_paragraphs': ['id', 'abstract_id', 'paragraph_id',
                                                                               'paragraph_label', 'paragraph_text'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_abstracts': ['id', 'abstract_id', 'abstract_lang_id',
                                                                     'abstract_type', 'provider',
                                                                     'copyright_information', 'paragraph_count'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_address_names': ['id', 'addr_id', 'name_id', 'addr_no_raw',
                                                                         'seq_no', 'role', 'reprint', 'lang_id',
                                                                         'addr_no', 'r_id', 'r_id_tr', 'orcid_id',
                                                                         'orcid_id_tr', 'dais_id', 'display',
                                                                         'display_name', 'full_name', 'wos_standard',
                                                                         'prefix', 'first_name', 'middle_name',
                                                                         'initials', 'last_name', 'suffix'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_address_names_email_addr': ['id', 'addr_id', 'name_id',
                                                                                    'email_id', 'email_addr',
                                                                                    'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_address_organizations': ['id', 'addr_id', 'org_id',
                                                                                 'organization', 'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_address_suborganizations': ['id', 'addr_id', 'suborg_id',
                                                                                    'suborganization', 'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_address_zip': ['id', 'addr_id', 'zip_id', 'zip', 'lang_id',
                                                                       'location'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_addresses': ['id', 'addr_id', 'addr_type', 'addr_no',
                                                                     'full_address', 'full_address_lang_id',
                                                                     'organization_count', 'suborganization_count',
                                                                     'url_type', 'url_date_info', 'url_create_date',
                                                                     'url_revised_date', 'url_cited_date', 'url',
                                                                     'laboratory', 'laboratory_lang_id', 'street',
                                                                     'street_lang_id', 'city', 'city_lang_id',
                                                                     'province', 'province_lang_id', 'state',
                                                                     'state_lang_id', 'country', 'country_lang_id',
                                                                     'post_num', 'post_num_lang_id', 'name_count'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_book_desc': ['id', 'desc_id', 'bk_binding', 'bk_publisher',
                                                                     'amount', 'currency', 'price_desc',
                                                                     'price_volumes', 'bk_prepay', 'bk_ordering'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_book_notes': ['id', 'note_id', 'book_note'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conf_date': ['id', 'conf_record_id', 'date_id', 'conf_date',
                                                                     'conf_start', 'conf_end', 'display_date',
                                                                     'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conf_info': ['id', 'conf_record_id', 'info_id', 'conf_info',
                                                                     'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conf_location': ['id', 'conf_record_id', 'location_id',
                                                                         'composite_location', 'composite_lang_id',
                                                                         'conf_host', 'host_lang_id', 'conf_city',
                                                                         'city_lang_id', 'conf_state', 'state_lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conf_sponsor': ['id', 'conf_record_id', 'sponsor_id', 'sponsor',
                                                                        'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conf_title': ['id', 'conf_record_id', 'title_id', 'conf_title',
                                                                      'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_conference': ['id', 'conf_record_id', 'conf_id',
                                                                      'conf_info_count', 'conf_title_count',
                                                                      'conf_date_count', 'conf_location_count',
                                                                      'sponsor_count', 'conf_type', 'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_contributors': ['id', 'contrib_id', 'role', 'reprint',
                                                                        'lang_id', 'addr_no', 'r_id', 'r_id_tr',
                                                                        'orcid_id', 'orcid_id_tr', 'dais_id', 'display',
                                                                        'seq_no', 'display_name', 'full_name',
                                                                        'wos_standard', 'prefix', 'first_name',
                                                                        'middle_name', 'initials', 'last_name',
                                                                        'suffix'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_doctypes': ['id', 'doctype_id', 'doctype', 'code'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_dynamic_citation_topics': ['id', 'dynamic_id', 'content_id',
                                                                                   'content_type', 'content'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_dynamic_identifiers': ['id', 'dynamic_id', 'identifier_type',
                                                                               'identifier_value', 'self_ind'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_edition': ['id', 'edition_ctr', 'edition'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_grant_agencies': ['id', 'grant_id', 'agency_id',
                                                                          'grant_agency_preferred', 'grant_agency'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_grant_ids': ['id', 'grant_id', 'id_id', 'grant_identifier'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_grants': ['id', 'grant_id', 'grant_info', 'grant_info_lang_id',
                                                                  'grant_agency', 'grant_agency_lang_id',
                                                                  'grant_agency_preferred', 'alt_agency_count',
                                                                  'grant_id_count', 'country', 'acronym',
                                                                  'investigator'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_headings': ['id', 'heading_id', 'heading'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_keywords': ['id', 'keyword_id', 'keyword', 'keyword_lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_keywords_plus': ['id', 'keyword_id', 'keyword_plus',
                                                                         'keyword_lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_languages': ['id', 'language_id', 'language', 'language_type',
                                                                     'status'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_normalized_doctypes': ['id', 'doctype_id', 'doctype', 'code'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_normalized_languages': ['id', 'language_id', 'language',
                                                                                'language_type', 'status'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_page': ['id', 'page_id', 'page_value', 'page_begin', 'page_end',
                                                                'page_count'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_publisher': ['id', 'publisher_id', 'addr_type', 'addr_no',
                                                                     'full_address', 'full_address_lang_id',
                                                                     'organization_count', 'suborganization_count',
                                                                     'email_addr_count', 'url_type', 'url_date_info',
                                                                     'url_create_date', 'url_revised_date',
                                                                     'url_cited_date', 'url', 'doi_count', 'laboratory',
                                                                     'laboratory_lang_id', 'street', 'street_lang_id',
                                                                     'city', 'city_lang_id', 'province',
                                                                     'province_lang_id', 'state', 'state_lang_id',
                                                                     'country', 'country_lang_id', 'post_num',
                                                                     'post_num_lang_id', 'name_count'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_publisher_names': ['id', 'publisher_id', 'name_id', 'role',
                                                                           'seq_no', 'reprint', 'lang_id',
                                                                           'addr_no_raw', 'r_id', 'r_id_tr', 'orcid_id',
                                                                           'orcid_id_tr', 'dais_id', 'display',
                                                                           'display_name', 'full_name', 'wos_standard',
                                                                           'prefix', 'first_name', 'middle_name',
                                                                           'initials', 'last_name', 'suffix'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_reference_section': ['id', 'ref_ctr', 'physicalLocation',
                                                                             'section', 'function', 'ref_ctrphs'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_references': ['id', 'ref_ctr', 'ref_id', 'occurenceorder',
                                                                      'cited_author', 'assignee', 'year', 'page',
                                                                      'volume', 'cited_title', 'cited_work', 'doi',
                                                                      'art_no', 'patent_no'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_reviewed_authors': ['id', 'author_id', 'author'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_reviewed_languages': ['id', 'language_id', 'language',
                                                                              'language_type', 'status'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_subheadings': ['id', 'subheading_id', 'subheading'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_subjects': ['id', 'subject_id', 'subject', 'ascatype', 'code',
                                                                    'edition'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_summary': ['id', 'file_number', 'coll_id', 'pubyear', 'season',
                                                                   'pubmonth', 'pubday', 'coverdate', 'edate', 'vol',
                                                                   'issue', 'voliss', 'supplement', 'special_issue',
                                                                   'part_no', 'pubtype', 'medium', 'model', 'indicator',
                                                                   'inpi', 'is_archive', 'city', 'country',
                                                                   'has_abstract', 'sortdate', 'title_count',
                                                                   'name_count',
                                                                   'doctype_count', 'conference_count',
                                                                   'language_count', 'normalized_language_count',
                                                                   'normalized_doctype_count',
                                                                   'descriptive_ref_count', 'refs_count',
                                                                   'reference_count', 'address_count', 'headings_count',
                                                                   'subheadings_count', 'subjects_count', 'fund_ack',
                                                                   'grants_count', 'grants_complete', 'keyword_count',
                                                                   'abstract_count', 'item_coll_id', 'item_ids',
                                                                   'item_ids_avail', 'bib_id', 'bib_pagecount',
                                                                   'bib_pagecount_type',
                                                                   'reviewed_language_count', 'reviewed_author_count',
                                                                   'reviewed_year', 'keywords_plus_count',
                                                                   'book_chapters',
                                                                   'book_pages', 'book_notes_count',
                                                                   'chapterlist_count', 'contributor_count'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_summary_names': ['id', 'name_id', 'role', 'seq_no',
                                                                         'addr_no_raw', 'reprint', 'lang_id', 'r_id',
                                                                         'r_id_tr',
                                                                         'orcid_id', 'orcid_id_tr', 'dais_id',
                                                                         'display', 'display_name', 'full_name',
                                                                         'wos_standard',
                                                                         'prefix', 'first_name', 'middle_name',
                                                                         'initials', 'last_name', 'suffix'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_summary_names_email_addr': ['id', 'name_id', 'email_id',
                                                                                    'email_addr', 'lang_id'],
    'clarivate-datapipline-project.bq_wos_2024_data.wos_titles': ['id', 'title_id', 'title', 'title_type', 'lang_id',
                                                                  'translated', 'non_english']
}

sql_directory = '/Users/macbook/Documents/GitHub/XML4UW/generic_parser-master/examples/web-of-science/output'
csv_directory = '/Users/macbook/Documents/GitHub/XML4UW/generic_parser-master/examples/web-of-science/output_csv'
error_log_path = '/Users/macbook/Documents/GitHub/XML4UW/generic_parser-master/examples/web-of-science/csv_log/mismatch_errors.log'  # The path to save error logs
mismatch_log_path = '/Users/macbook/Documents/GitHub/XML4UW/generic_parser-master/examples/web-of-science/csv_log/mismatch_queries.log'  # The path to save error queries

# Run the processing
process_sql_files(sql_directory, csv_directory, table_schemas, error_log_path, mismatch_log_path)
