from telethon.sync import TelegramClient

api_id = 35077722
api_hash = "b11b84ff80ecd6905230452bd382684f"

client = TelegramClient("data/aggregator", api_id, api_hash)
client.start()

print("LOGIN OK")