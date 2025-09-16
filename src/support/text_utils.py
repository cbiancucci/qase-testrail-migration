"""
Text utilities for TestRail to Qase migration.
Contains functions for text formatting and conversion.
"""

import re


def convert_testrail_tables_to_markdown(text):
    """
    Convert TestRail table format to Markdown format.
    
    TestRail format:
    |||:Remote|:Shipped with Device
    ||Giga remote|  Giga
    ||Laredo  |  Austin
    
    Markdown format:
    | Remote | Shipped with Device |
    |--------|-------------------|
    | Giga remote | Giga |
    | Laredo | Austin |
    
    Note: The colon (:) prefix in TestRail column headers is automatically removed.
    
    Args:
        text (str): Text that may contain TestRail table format
        
    Returns:
        str: Text with TestRail tables converted to Markdown format
    """
    if text is None:
        return text
        
    # Pattern to match TestRail table format
    # Matches lines starting with ||| (header) or || (data rows)
    table_pattern = r'(\|\|\|.*?\n(?:\|\|.*?\n)*)'
    
    def convert_table(match):
        table_content = match.group(1).strip()
        lines = table_content.split('\n')
        
        if len(lines) < 2:  # Need at least header and one data row
            return table_content
            
        # Process header line (starts with |||)
        header_line = lines[0]
        if not header_line.startswith('|||'):
            return table_content
            
        # Extract header columns (remove ||| prefix and split by |)
        header_cols = [col.strip().lstrip(':') for col in header_line[3:].split('|') if col.strip()]
        
        # Create markdown table header
        markdown_table = '| ' + ' | '.join(header_cols) + ' |\n'
        markdown_table += '|' + '|'.join(['-' * (len(col) + 2) for col in header_cols]) + '|\n'
        
        # Process data rows (start with ||)
        for line in lines[1:]:
            if line.startswith('||'):
                # Extract data columns (remove || prefix and split by |)
                data_cols = [col.strip() for col in line[2:].split('|')]
                # Ensure we have the right number of columns
                while len(data_cols) < len(header_cols):
                    data_cols.append('')
                # Truncate if we have too many columns
                data_cols = data_cols[:len(header_cols)]
                markdown_table += '| ' + ' | '.join(data_cols) + ' |\n'
        
        return markdown_table
    
    # Replace all table patterns
    converted_text = re.sub(table_pattern, convert_table, text, flags=re.MULTILINE)
    
    return converted_text


def format_links_as_markdown(text):
    """
    Format text by converting TestRail tables to Markdown and formatting URLs as Markdown links.
    
    Args:
        text (str): Text to format
        
    Returns:
        str: Formatted text with tables converted and URLs as Markdown links
    """
    if text is None:
        return None

    # First convert TestRail tables to Markdown
    text = convert_testrail_tables_to_markdown(text)

    # Fix numbering
    text = fix_numbering(text)

    # Format URLs as Markdown links
    url_pattern = re.compile(r'(?<!\]\()(?<!\])\b(http[s]?://[^\s]+)')
    formatted_text = url_pattern.sub(r'[\1](\1)', text)

    return formatted_text


def fix_numbering(text):
    """
    Fix numbering in text by converting 0-based numbering to 1-based numbering.
    
    This function finds lines that start with a number followed by a dot and space,
    and converts them from 0-based to 1-based numbering. Each separate block of
    numbered lines is numbered independently starting from 1.
    
    Example:
    Input:
    0. Select Settings
    0. Select Parental controls.
    Some unnumbered text here
    0. Enter a pin code such as 1111.
    0. Select OK.
    
    Output:
    1. Select Settings
    2. Select Parental controls.
    Some unnumbered text here
    1. Enter a pin code such as 1111.
    2. Select OK.
    
    Args:
        text (str): Text that may contain 0-based numbered lines
        
    Returns:
        str: Text with corrected 1-based numbering
    """
    if text is None:
        return None
    
    lines = text.split('\n')
    result_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line starts with a number followed by a dot and space
        numbering_match = re.match(r'^(\d+)\. ', line)
        
        if numbering_match:
            # Found a numbered line, process the entire block of numbered lines
            block_start = i
            block_number = 1  # Start numbering from 1 for this block
            
            # Process all consecutive numbered lines
            while i < len(lines):
                current_line = lines[i]
                current_match = re.match(r'^(\d+)\. ', current_line)
                
                if current_match:
                    # Replace the number with the new sequential number
                    new_line = re.sub(r'^\d+\. ', f'{block_number}. ', current_line)
                    result_lines.append(new_line)
                    block_number += 1
                    i += 1
                else:
                    # Non-numbered line, break the block
                    break
        else:
            # Non-numbered line, add as is
            result_lines.append(line)
            i += 1
    
    return '\n'.join(result_lines)
