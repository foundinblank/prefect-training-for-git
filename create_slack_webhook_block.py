from prefect.blocks.notifications import SlackWebhook

webhook_block = SlackWebhook(
    url="https://hooks.slack.com/services/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"
)

webhook_block.save("slack-prefect-channel", overwrite=True)


# https://hooks.slack.com/workflows/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