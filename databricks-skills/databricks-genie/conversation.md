# Genie Conversations

Use the Genie Conversation API to ask natural language questions to a curated Genie Space.

## Overview

The `aidevkit genie ask` command allows you to programmatically send questions to a Genie Space and receive SQL-generated answers. Instead of writing SQL directly, you delegate the query generation to Genie, which has been curated with business logic, instructions, and certified queries.

## When to Use `aidevkit genie ask`

### Use `aidevkit genie ask` When:

| Scenario | Why |
|----------|-----|
| Genie Space has curated business logic | Genie knows rules like "active customer = ordered in 90 days" |
| User explicitly says "ask Genie" or "use my Genie Space" | User intent to use their curated space |
| Complex business metrics with specific definitions | Genie has certified queries for official metrics |
| Testing a Genie Space after creating it | Validate the space works correctly |
| User wants conversational data exploration | Genie handles context for follow-up questions |

### Use Direct SQL (`aidevkit sql execute`) Instead When:

| Scenario | Why |
|----------|-----|
| Simple ad-hoc query | Direct SQL is faster, no curation needed |
| You already have the exact SQL | No need for Genie to regenerate |
| Genie Space doesn't exist for this data | Can't use Genie without a space |
| Need precise control over the query | Direct SQL gives exact control |

## CLI Commands

| Command | Purpose |
|---------|---------|
| `aidevkit genie ask` | Ask a question or follow-up (`--conversation-id` optional) |

## Basic Usage

### Ask a Question

```bash
aidevkit genie ask --space-id "01abc123..." --question "What were total sales last month?"
```

**Response (JSON):**
```json
{
    "question": "What were total sales last month?",
    "conversation_id": "conv_xyz789",
    "message_id": "msg_123",
    "status": "COMPLETED",
    "sql": "SELECT SUM(total_amount) AS total_sales FROM orders WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH) AND order_date < DATE_TRUNC('month', CURRENT_DATE)",
    "columns": ["total_sales"],
    "data": [[125430.50]],
    "row_count": 1
}
```

### Ask Follow-up Questions

Use the `conversation_id` from the first response to ask follow-up questions with context:

```bash
# First question - capture conversation_id from output
aidevkit genie ask --space-id "01abc123..." \
    --question "What were total sales last month?" > result.json

# Extract conversation_id
CONV_ID=$(jq -r '.conversation_id' result.json)

# Follow-up (uses context from first question)
aidevkit genie ask --space-id "01abc123..." \
    --question "Break that down by region" \
    --conversation-id "$CONV_ID"
```

Genie remembers the context, so "that" refers to "total sales last month".

## Response Fields

| Field | Description |
|-------|-------------|
| `question` | The original question asked |
| `conversation_id` | ID for follow-up questions |
| `message_id` | Unique message identifier |
| `status` | `COMPLETED`, `FAILED`, `CANCELLED`, `TIMEOUT` |
| `sql` | The SQL query Genie generated |
| `columns` | List of column names in result |
| `data` | Query results as list of rows |
| `row_count` | Number of rows returned |
| `text_response` | Text explanation (if Genie asks for clarification) |
| `error` | Error message (if status is not COMPLETED) |

## Handling Responses

### Successful Response

```bash
# Ask a question and check the response
aidevkit genie ask --space-id "$SPACE_ID" --question "Who are our top 10 customers?" > result.json

# Check status and extract data
STATUS=$(jq -r '.status' result.json)
if [ "$STATUS" = "COMPLETED" ]; then
    echo "SQL: $(jq -r '.sql' result.json)"
    echo "Rows: $(jq -r '.row_count' result.json)"
    jq '.data' result.json
fi
```

### Failed Response

```bash
aidevkit genie ask --space-id "$SPACE_ID" --question "What is the meaning of life?" > result.json

STATUS=$(jq -r '.status' result.json)
if [ "$STATUS" = "FAILED" ]; then
    echo "Error: $(jq -r '.error' result.json)"
    # Genie couldn't answer - may need to rephrase or use direct SQL
fi
```

### Timeout

```bash
# Specify a timeout
aidevkit genie ask --space-id "$SPACE_ID" --question "$QUESTION" --timeout 60 > result.json

STATUS=$(jq -r '.status' result.json)
if [ "$STATUS" = "TIMEOUT" ]; then
    echo "Query took too long - try a simpler question or increase timeout"
fi
```

## Example Workflows

### Workflow 1: User Asks to Use Genie

```
User: "Ask my Sales Genie what the churn rate is"

Claude:
1. Identifies user wants to use Genie (explicit request)
2. Runs: aidevkit genie ask --space-id "sales_genie_id" --question "What is the churn rate?"
3. Returns: "Based on your Sales Genie, the churn rate is 4.2%.
   Genie used this SQL: SELECT ..."
```

### Workflow 2: Testing a New Genie Space

```
User: "I just created a Genie Space for HR data. Can you test it?"

Claude:
1. Gets the space_id from the user or recent `aidevkit genie create-or-update` result
2. Runs test questions:
   - aidevkit genie ask --space-id "$ID" --question "How many employees do we have?"
   - aidevkit genie ask --space-id "$ID" --question "What is the average salary by department?"
3. Reports results: "Your HR Genie is working. It correctly answered..."
```

### Workflow 3: Data Exploration with Follow-ups

```
User: "Use my analytics Genie to explore sales trends"

Claude:
1. aidevkit genie ask --space-id "$ID" --question "What were total sales by month this year?"
2. User: "Which month had the highest growth?"
3. aidevkit genie ask --space-id "$ID" --question "Which month had the highest growth?" --conversation-id "$CONV_ID"
4. User: "What products drove that growth?"
5. aidevkit genie ask --space-id "$ID" --question "What products drove that growth?" --conversation-id "$CONV_ID"
```

## Best Practices

### Start New Conversations for New Topics

Don't reuse conversations across unrelated questions:

```bash
# Good: New conversation for new topic
aidevkit genie ask --space-id "$SPACE_ID" --question "What were sales last month?"  # New conversation
aidevkit genie ask --space-id "$SPACE_ID" --question "How many employees do we have?"  # New conversation

# Good: Follow-up for related question
aidevkit genie ask --space-id "$SPACE_ID" --question "What were sales last month?" > result.json
CONV_ID=$(jq -r '.conversation_id' result.json)
aidevkit genie ask --space-id "$SPACE_ID" \
    --question "Break that down by product" \
    --conversation-id "$CONV_ID"  # Related follow-up
```

### Handle Clarification Requests

Genie may ask for clarification instead of returning results:

```bash
aidevkit genie ask --space-id "$SPACE_ID" --question "Show me the data" > result.json

TEXT_RESPONSE=$(jq -r '.text_response // empty' result.json)
if [ -n "$TEXT_RESPONSE" ]; then
    # Genie is asking for clarification
    echo "Genie asks: $TEXT_RESPONSE"
    # Rephrase with more specifics
fi
```

### Set Appropriate Timeouts

- Simple aggregations: 30-60 seconds
- Complex joins: 60-120 seconds
- Large data scans: 120+ seconds

```bash
# Quick question
aidevkit genie ask --space-id "$SPACE_ID" --question "How many orders today?" --timeout 30

# Complex analysis
aidevkit genie ask --space-id "$SPACE_ID" \
    --question "Calculate customer lifetime value for all customers" \
    --timeout 180
```

## Troubleshooting

### "Genie Space not found"

- Verify the `space_id` is correct
- Check you have access to the space
- Use `aidevkit genie get --space-id ...` to verify it exists

### "Query timed out"

- Increase `timeout_seconds`
- Simplify the question
- Check if the SQL warehouse is running

### "Failed to generate SQL"

- Rephrase the question more clearly
- Check if the question is answerable with the available tables
- Add more instructions/curation to the Genie Space

### Unexpected Results

- Review the generated SQL in the response
- Add SQL instructions to the Genie Space via the Databricks UI
- Add sample questions that demonstrate correct patterns
