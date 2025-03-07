import json
import redis
from mongoengine import connect, Document, StringField, ListField, ReferenceField
from pymongo import MongoClient
from bson.objectid import ObjectId
import re

# Підключення до MongoDB
connect(host="mongodb+srv://your_mongodb_uri")

# Підключення до Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Моделі для MongoDB
class Author(Document):
    fullname = StringField(required=True)
    born_date = StringField()
    born_location = StringField()
    description = StringField()

class Quote(Document):
    tags = ListField(StringField())
    author = ReferenceField(Author, reverse_delete_rule=2)
    quote = StringField(required=True)

# Функція завантаження даних у MongoDB
def load_data():
    with open("authors.json", "r", encoding="utf-8") as file:
        authors_data = json.load(file)

    with open("quotes.json", "r", encoding="utf-8") as file:
        quotes_data = json.load(file)

    author_map = {}
    for author in authors_data:
        existing_author = Author.objects(fullname=author["fullname"]).first()
        if not existing_author:
            new_author = Author(**author).save()
            author_map[author["fullname"]] = new_author.id
        else:
            author_map[author["fullname"]] = existing_author.id

    for quote in quotes_data:
        existing_quote = Quote.objects(quote=quote["quote"]).first()
        if not existing_quote:
            Quote(
                tags=quote["tags"],
                author=Author.objects.get(id=author_map[quote["author"]]),
                quote=quote["quote"]
            ).save()

# Функція пошуку автора з можливістю скороченого запису та кешування
def find_quotes_by_author(author_name):
    cache_key = f"name:{author_name}"
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        print("🔹 Взято з кешу Redis 🔹")
        print(cached_result)
        return

    pattern = re.compile(f"^{author_name}", re.IGNORECASE)
    authors = Author.objects(fullname=pattern)

    if not authors:
        print("Автор не знайдений")
        return

    for author in authors:
        quotes = Quote.objects(author=author)
        result = "\n".join([quote.quote for quote in quotes])
        print(result)
        redis_client.set(cache_key, result, ex=300)  # Кешування на 5 хвилин

# Функція пошуку цитат за тегом із кешуванням та підтримкою скороченого запису
def find_quotes_by_tag(tag_name):
    cache_key = f"tag:{tag_name}"
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        print("🔹 Взято з кешу Redis 🔹")
        print(cached_result)
        return

    pattern = re.compile(f"^{tag_name}", re.IGNORECASE)
    quotes = Quote.objects(tags=pattern)

    if not quotes:
        print("Цитати за тегом не знайдені")
        return

    result = "\n".join([quote.quote for quote in quotes])
    print(result)
    redis_client.set(cache_key, result, ex=300)  # Кешування на 5 хвилин

# CLI для пошуку
def cli():
    while True:
        command = input("Введіть команду (name: | tag: | exit): ").strip()
        if command.lower() == "exit":
            print("Вихід з програми.")
            break
        elif command.startswith("name:"):
            author_name = command[5:].strip()
            find_quotes_by_author(author_name)
        elif command.startswith("tag:"):
            tag_name = command[4:].strip()
            find_quotes_by_tag(tag_name)
        else:
            print("Невідома команда, спробуйте ще раз.")

if __name__ == "__main__":
    load_data()
    cli()
