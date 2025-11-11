#!/usr/bin/env python3
"""
Simple OCR-based transaction extractor - just raw data to CSV/JSON
"""

import subprocess
import tempfile
import shutil
from pathlib import Path
import json
import csv
import re


def check_dependencies():
    """Check if OCR tools are installed"""
    required = ['gs', 'pdftoppm', 'magick', 'ocrmypdf', 'pdftotext']

    missing = [cmd for cmd in required if not shutil.which(cmd)]

    if missing:
        print(f"\n❌ Missing: {', '.join(missing)}\n")
        print("Install with:")
        print("  brew install ghostscript poppler imagemagick")
        print("  pip3 install ocrmypdf")
        return False

    print("✓ All dependencies found\n")
    return True


def ocr_pdf(pdf_path, temp_dir):
    """Run OCR pipeline on PDF, return clean text"""
    print(f"  Running OCR on {pdf_path.name}...")

    base = pdf_path.stem
    temp_gs = temp_dir / f"{base}_gs.pdf"
    temp_recon = temp_dir / f"{base}_recon.pdf"
    ocr_pdf = temp_dir / f"{base}_ocr.pdf"

    try:
        # Ghostscript
        subprocess.run(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pdfwrite',
                       f'-sOutputFile={temp_gs}', str(pdf_path)],
                      check=True, capture_output=True)

        # To images
        page_prefix = temp_dir / f"{base}_page"
        subprocess.run(['pdftoppm', '-png', '-r', '300', str(temp_gs), str(page_prefix)],
                      check=True, capture_output=True)

        # Reconstruct PDF
        images = sorted(temp_dir.glob(f"{base}_page-*.png"))
        subprocess.run(['magick', *[str(img) for img in images], str(temp_recon)],
                      check=True, capture_output=True)

        # Cleanup images
        for img in images:
            img.unlink()

        # OCR
        subprocess.run(['ocrmypdf', '--language', 'eng', str(temp_recon), str(ocr_pdf)],
                      check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Extract text
        result = subprocess.run(['pdftotext', str(ocr_pdf), '-'],
                               capture_output=True, text=True, check=True)

        print(f"  ✓ OCR complete")
        return result.stdout

    except subprocess.CalledProcessError as e:
        print(f"  ✗ OCR failed: {e}")
        return None


def extract_statement_metadata(text):
    """Extract statement-level metadata (dates and balances)"""
    metadata = {
        'year': None,
        'statement_date': None,
        'period_from': None,
        'period_to': None,
        'opening_balance': None,
        'closing_balance': None
    }

    lines = [line.strip() for line in text.split('\n')]

    for i, line in enumerate(lines):
        # Look for statement period: "Account statement from DD.MM.YYYY to DD.MM.YYYY"
        period_match = re.search(r'from\s+(\d{2}\.\d{2}\.\d{4})\s+to\s+(\d{2}\.\d{2}\.\d{4})', line, re.IGNORECASE)
        if period_match:
            # Convert DD.MM.YYYY to YYYY-MM-DD
            from_date = period_match.group(1).split('.')
            to_date = period_match.group(2).split('.')
            metadata['period_from'] = f"{from_date[2]}-{from_date[1]}-{from_date[0]}"
            metadata['period_to'] = f"{to_date[2]}-{to_date[1]}-{to_date[0]}"
            metadata['statement_date'] = metadata['period_to']  # Statement date is typically the end date
            metadata['year'] = to_date[2]  # Year from the end date

        # Look for "Previous balance" - the balance appears within next 25 lines
        if 'previous balance' in line.lower() and not metadata['opening_balance']:
            # After "Previous balance", look for first amount matching balance pattern
            for j in range(i+1, min(i+25, len(lines))):
                balance_match = re.match(r'^([-+]?\s*[\d,]+\.\d{2})$', lines[j])
                if balance_match:
                    metadata['opening_balance'] = balance_match.group(1).strip()
                    break

        # Look for "New balance" - the balance appears within next 25 lines
        if 'new balance' in line.lower() and not metadata['closing_balance']:
            # After "New balance", look for first amount matching balance pattern
            for j in range(i+1, min(i+25, len(lines))):
                balance_match = re.match(r'^([-+]?\s*[\d,]+\.\d{2})$', lines[j])
                if balance_match:
                    metadata['closing_balance'] = balance_match.group(1).strip()
                    break

    return metadata


def extract_transactions(text, period_from=None, period_to=None):
    """Extract transactions from OCR'd bank statement table"""
    transactions = []
    lines = [line.strip() for line in text.split('\n')]

    # Determine year from statement period
    # Transactions span from period_from to period_to, may cross year boundary
    year_from = None
    year_to = None
    if period_from:
        year_from = int(period_from.split('-')[0])
    if period_to:
        year_to = int(period_to.split('-')[0])

    i = 0
    while i < len(lines):
        line = lines[i]

        # Look for booking date (MM/DD format)
        if re.match(r'^\d{2}/\d{2}$', line):
            booking_date_mm_dd = line
            i += 1

            # Next should be value date (or skip blank lines)
            while i < len(lines) and not lines[i]:
                i += 1

            if i >= len(lines) or not re.match(r'^\d{2}/\d{2}$', lines[i]):
                continue

            value_date_mm_dd = lines[i]
            i += 1

            # Collect description and look for amount
            description_parts = []
            amount = None

            while i < len(lines):
                next_line = lines[i]

                # Stop at next transaction date
                if re.match(r'^\d{2}/\d{2}$', next_line):
                    break

                # Stop at page markers
                if next_line in ['Statement Page', 'Branch number', 'New balance', 'Important notes', 'German bank code']:
                    break

                # Check if it's an amount (with optional sign, commas, and 2 decimals)
                # Examples: "- 12.01", "+ 1,109.68", "1,112.00"
                if re.match(r'^[-+]?\s*[\d,]+\.\d{2}$', next_line):
                    amount = next_line.strip()
                    i += 1
                    # Continue collecting description after amount
                    continue

                # Collect non-empty lines as description
                if next_line:
                    description_parts.append(next_line)

                i += 1

            # Save transaction if we have all required fields
            description = ' '.join(description_parts).strip()

            if description and amount:
                # Convert MM/DD to YYYY-MM-DD format
                booking_date = convert_mm_dd_to_full_date(booking_date_mm_dd, period_from, period_to)
                value_date = convert_mm_dd_to_full_date(value_date_mm_dd, period_from, period_to)

                transactions.append({
                    'booking_date': booking_date,
                    'value_date': value_date,
                    'description': description,
                    'amount': amount
                })
        else:
            i += 1

    return transactions


def convert_mm_dd_to_full_date(mm_dd, period_from, period_to):
    """Convert MM/DD format to YYYY-MM-DD using statement period"""
    if not mm_dd or not period_from or not period_to:
        return mm_dd

    try:
        month, day = mm_dd.split('/')
        month = int(month)
        day = int(day)

        # Parse period dates
        from_year, from_month, from_day = map(int, period_from.split('-'))
        to_year, to_month, to_day = map(int, period_to.split('-'))

        # Determine which year the transaction belongs to
        # If statement crosses year boundary, use logic to assign correct year
        if from_year == to_year:
            # Statement within single year
            year = from_year
        else:
            # Statement crosses year boundary
            # If month >= from_month, use from_year; else use to_year
            if month >= from_month:
                year = from_year
            else:
                year = to_year

        return f"{year}-{month:02d}-{day:02d}"
    except:
        return mm_dd


def main():
    print("\n" + "="*80)
    print(" "*25 + "OCR Transaction Extractor")
    print("="*80 + "\n")

    # Check dependencies
    if not check_dependencies():
        return

    # Find PDFs - use relative path from script location
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    reports_dir = script_dir.parent / 'reports'

    # Collect all Account_statement PDFs from all year folders
    pdf_files = []
    for year_folder in sorted(data_dir.glob('*')):
        if year_folder.is_dir():
            year_pdfs = sorted(year_folder.glob('Account_statement_*.pdf'))
            if year_pdfs:
                print(f"Found {len(year_pdfs)} PDFs in {year_folder.name}/")
                pdf_files.extend(year_pdfs)

    if not pdf_files:
        print("No Account_statement_*.pdf files found in data/ folders")
        return

    print(f"\nTotal: {len(pdf_files)} PDFs to process\n")

    all_transactions = []
    all_statements = []
    temp_dir = Path(tempfile.mkdtemp(prefix='bank_ocr_'))

    try:
        for pdf_file in pdf_files:
            print(f"Processing: {pdf_file.name}")

            # OCR
            text = ocr_pdf(pdf_file, temp_dir)

            if not text:
                print("  Skipping - OCR failed\n")
                continue

            # Extract metadata
            metadata = extract_statement_metadata(text)
            print(f"  Statement: {metadata['period_from']} to {metadata['period_to']}")
            print(f"  Opening: {metadata['opening_balance']}, Closing: {metadata['closing_balance']}")

            # Extract transactions with period info for date conversion
            transactions = extract_transactions(text, metadata['period_from'], metadata['period_to'])
            print(f"  Found {len(transactions)} transactions\n")

            all_transactions.extend(transactions)
            all_statements.append(metadata)

    finally:
        shutil.rmtree(temp_dir)
        print(f"✓ Cleaned up temp files\n")

    print("="*80)
    print(f"TOTAL: {len(all_transactions)} transactions from {len(all_statements)} statements\n")

    # Ensure reports directory exists
    reports_dir.mkdir(exist_ok=True)

    # Export transactions to JSON
    json_file = reports_dir / 'transactions.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(all_transactions, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved to {json_file}")

    # Export transactions to CSV
    csv_file = reports_dir / 'transactions.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['booking_date', 'value_date', 'description', 'amount'])
        writer.writeheader()
        writer.writerows(all_transactions)
    print(f"✓ Saved to {csv_file}")

    # Export statements to JSON
    statements_json_file = reports_dir / 'statements.json'
    with open(statements_json_file, 'w', encoding='utf-8') as f:
        json.dump(all_statements, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved to {statements_json_file}")

    # Export statements to CSV
    statements_csv_file = reports_dir / 'statements.csv'
    with open(statements_csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['year', 'statement_date', 'period_from', 'period_to', 'opening_balance', 'closing_balance'])
        writer.writeheader()
        writer.writerows(all_statements)
    print(f"✓ Saved to {statements_csv_file}\n")


if __name__ == '__main__':
    main()
