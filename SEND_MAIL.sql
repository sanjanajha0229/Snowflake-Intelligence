CREATE OR REPLACE PROCEDURE SNOWFLAKE_INTELLIGENCE_DB.PROCUREMENT_DATASET.SEND_MAIL("RECIPIENT" VARCHAR, "SUBJECT" VARCHAR, "TEXT" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_mail'
EXECUTE AS OWNER
AS '
def send_mail(session, recipient, subject, text):
    session.call(
        ''SYSTEM$SEND_EMAIL'',
        ''ai_email_int'',
        recipient,
        subject,
        text,
        ''text/html''
    )
    return f''Email was sent to {recipient} with subject: "{subject}".''
';
