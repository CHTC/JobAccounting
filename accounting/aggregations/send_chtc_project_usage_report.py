import argparse
from pathlib import Path

from functions import send_email
from metric_functions import EMAIL_ARGS, valid_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a CHTC project usage report email from a pre-generated HTML file."
    )

    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)

    parser.add_argument("--project", required=True)
    parser.add_argument("--start", type=valid_date, required=True)
    parser.add_argument("--end", type=valid_date, required=True)
    parser.add_argument(
        "--html-file",
        type=Path,
        required=True,
        help="HTML file to use as the email body.",
    )
    parser.add_argument(
        "--attach",
        action="append",
        default=[],
        type=Path,
        metavar="FILE",
        help="File to attach to the email (can be specified multiple times).",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    days = (args.end - args.start).days
    subject = f"{days}-day CHTC Usage Report for {args.project} starting {args.start.strftime('%Y-%m-%d')}"

    send_email(
        subject=subject,
        from_addr=args.from_addr,
        to_addrs=args.to,
        html=args.html_file.read_text(),
        cc_addrs=args.cc,
        bcc_addrs=args.bcc,
        reply_to_addr=args.reply_to,
        attachments=args.attach,
        smtp_server=args.smtp_server,
        smtp_username=args.smtp_username,
        smtp_password_file=args.smtp_password_file,
    )


if __name__ == "__main__":
    main()
