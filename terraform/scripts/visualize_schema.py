import lxml.etree as etree
import sys
import os

def generate_dot(config_path, output_path):
    """
    Parses wos_config.xml and generates a DOT file for Graphviz.
    Each table is a node, and nesting implies a relationship.
    """
    if not os.path.exists(config_path):
        print(f"Error: {config_path} not found.")
        return

    try:
        tree = etree.parse(config_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return

    # To store relationships: (parent_table, child_table)
    relationships = []
    # To store all unique tables found
    tables = set()

    def find_tables(node, current_table=None):
        table_name = node.get("table")
        
        # If this node defines a table
        if table_name:
            tables.add(table_name)
            if current_table and current_table != table_name:
                relationships.append((current_table, table_name))
            
            # The new current_table for children is this table
            new_current_table = table_name
        else:
            # Continue with the existing current_table
            new_current_table = current_table
            
        for child in node:
            find_tables(child, new_current_table)

    find_tables(root)

    # Generate DOT content
    dot_lines = ["digraph WOS_Schema {"]
    dot_lines.append("  rankdir=LR;")
    dot_lines.append("  node [shape=box, style=filled, fillcolor=lightblue, fontname=\"Arial\"];")
    dot_lines.append("  edge [fontname=\"Arial\"];")
    dot_lines.append("")

    # Add nodes
    for table in sorted(list(tables)):
        dot_lines.append(f'  "{table}" [label="{table}"];')

    dot_lines.append("")

    # Add edges
    for parent, child in sorted(list(set(relationships))):
        dot_lines.append(f'  "{parent}" -> "{child}";')

    dot_lines.append("}")

    dot_content = "\n".join(dot_lines)
    with open(output_path, "w") as f:
        f.write(dot_content)
    
    print(f"DOT file generated at: {output_path}")

    # Try to render using Python 'graphviz' library if available
    try:
        from graphviz import Source
        src = Source(dot_content)
        # format='png' will create 'wos_schema.png'
        src.render(filename=output_path.replace('.dot', ''), format='png', cleanup=True)
        print(f"Successfully rendered visualization as: {output_path.replace('.dot', '.png')}")
    except ImportError:
        print("\nNote: Python 'graphviz' library not found. To render locally, run: pip install graphviz")
        print("Or use a web renderer (see instructions below).")

if __name__ == "__main__":
    # Correct path relative to the script's root when run from project root
    config_file = "parser/wos_config.xml"
    output_file = "wos_schema.dot"
    generate_dot(config_file, output_file)
