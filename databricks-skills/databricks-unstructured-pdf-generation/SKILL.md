---
name: databricks-unstructured-pdf-generation
description: "Generate PDF documents from HTML and upload to Unity Catalog volumes. Use for creating test PDFs, demo documents, reports, or evaluation datasets."
---

# PDF Generation from HTML

Convert HTML content to PDF documents and upload them to Unity Catalog Volumes.

## Overview

Generate PDFs from HTML using `scripts/pdf_generator.py`. You generate the HTML content, write it to a temp file, then run the script to convert and upload.

## Installation

```bash
uv pip install plutoprint
```

## Usage

Run the script via CLI:

```bash
# Generate from HTML file (recommended - avoids shell escaping issues)
python scripts/pdf_generator.py generate --html-file ./raw_pdf/content.html \
    --filename report.pdf --catalog my_catalog --schema my_schema

# Generate from inline HTML (for simple content)
python scripts/pdf_generator.py generate --html '<html><body><h1>Hello</h1></body></html>' \
    --filename hello.pdf --catalog my_catalog --schema my_schema

# With subfolder and JSON output
python scripts/pdf_generator.py generate --html-file ./raw_pdf/content.html \
    --filename report.pdf --catalog my_catalog --schema my_schema --folder reports --json
```

**Workflow:**
1. Write HTML content to a local file (e.g., `./raw_pdf/content.html`)
2. Run the script with `--html-file`
3. Script converts to PDF and uploads to Unity Catalog volume

## Quick Start

1. Write HTML to a local file
2. Run the script

```bash
# Create folder and write HTML content
mkdir -p ./raw_pdf
cat > ./raw_pdf/report.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #1a73e8; }
    </style>
</head>
<body>
    <h1>Quarterly Report Q1 2024</h1>
    <p>Revenue increased 15% year-over-year...</p>
</body>
</html>
EOF

# Generate and upload PDF
python scripts/pdf_generator.py generate --html-file ./raw_pdf/report.html \
    --filename q1_report.pdf --catalog my_catalog --schema my_schema
```

## HTML Best Practices

### Use Complete HTML5 Structure

Always include the full HTML structure:

```html
<!DOCTYPE html>
<html>
<head>
    <style>
        /* Your CSS here */
    </style>
</head>
<body>
    <!-- Your content here -->
</body>
</html>
```

### CSS Features Supported

PlutoPrint supports modern CSS3:
- Flexbox and Grid layouts
- CSS variables (`--var-name`)
- Web fonts (system fonts recommended)
- Colors, backgrounds, borders
- Tables with styling

### CSS to Avoid

- Animations and transitions (static PDF)
- Interactive elements (forms, hover effects)
- External resources (images via URL) - use embedded base64 if needed

### Professional Document Template

```html
<!DOCTYPE html>
<html>
<head>
    <style>
        :root {
            --primary: #1a73e8;
            --text: #202124;
            --gray: #5f6368;
        }
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 50px;
            color: var(--text);
            line-height: 1.6;
        }
        h1 {
            color: var(--primary);
            border-bottom: 3px solid var(--primary);
            padding-bottom: 15px;
        }
        h2 { color: var(--text); margin-top: 30px; }
        .highlight {
            background: #e8f0fe;
            padding: 15px;
            border-left: 4px solid var(--primary);
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th, td {
            border: 1px solid #dadce0;
            padding: 12px;
            text-align: left;
        }
        th { background: #f1f3f4; }
        .footer {
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid #dadce0;
            color: var(--gray);
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <h1>Document Title</h1>

    <h2>Section 1</h2>
    <p>Content here...</p>

    <div class="highlight">
        <strong>Important:</strong> Key information highlighted here.
    </div>

    <h2>Data Table</h2>
    <table>
        <tr><th>Column 1</th><th>Column 2</th><th>Column 3</th></tr>
        <tr><td>Data</td><td>Data</td><td>Data</td></tr>
    </table>

    <div class="footer">
        Generated on 2024-01-15 | Confidential
    </div>
</body>
</html>
```

## Prerequisites

- Unity Catalog schema must exist
- Volume must exist (default: `raw_data`)
- User must have WRITE permission on the volume
- Databricks CLI must be configured and authenticated

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Volume does not exist" | Create the volume first or use an existing one |
| "Schema does not exist" | Create the schema or check the name |
| PDF looks wrong | Check HTML/CSS syntax, use supported CSS features |
| Slow generation | Call multiple PDFs in parallel, not sequentially |
| CLI not found | Ensure `databricks` CLI is installed and in PATH |
