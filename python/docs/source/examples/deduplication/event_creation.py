import json
import random
from datetime import datetime, timedelta

# Function to generate a random timestamp in the current day
def generate_timestamp(day: datetime) -> int:
    start_date = datetime(2023, 9, 22)
    end_date = start_date + timedelta(days=1)
    random_time = start_date + random.random() * (end_date - start_date)
    return int(random_time.timestamp())


# Function to generate a random grams_consumed value based on animal size
def generate_grams_consumed(animal: str) -> int:
    if animal in ["Elephant", "Bear", "Rhinoceros"]:
        return random.randint(2000, 4000)
    elif animal in ["Giraffe", "Zebra", "Kangaroo", "Flamingo", "Chimpanzee", "Koala", "Lemur"]:
        return random.randint(500, 1000)
    elif animal in ["Lion", "Tiger", "Gorilla", "Cheetah", "Alligator", "Snake", "Panda"]:
        return random.randint(100, 500)
    else:
        return random.randint(10, 100)

subsort = 1

# Function to generate feeding events for all animals in a single day
def generate_feeding_events(day: datetime) -> []:
    feeding_events = []
    for animal, names in animal_data.items():
        for name in names:
            global subsort
            feeding_event = {
                "timestamp": generate_timestamp(day),
                "subsort": subsort,
                "animal": animal,
                "name": name,
                "grams_consumed": generate_grams_consumed(animal)
            }
            feeding_events.append(feeding_event)
            subsort += 1

    # Sort the feeding events by timestamp
    feeding_events.sort(key=lambda x: x["timestamp"])
    return feeding_events


# Define the list of animals and their names
animal_data = {
    "Lion": ["Simba", "Nala", "Mufasa", "Sarabi"],
    "Tiger": ["Rajah", "Shere Khan", "Tigger"],
    "Elephant": ["Dumbo", "Ellie"],
    "Giraffe": ["Gigi", "Longneck", "Sophie", "Stretch", "Tallulah"],
    "Zebra": ["Zara", "Ziggy", "Stripes", "Zane", "Zoe"],
    "Gorilla": ["Koko", "Harambe"],
    "Penguin": ["Waddle", "Flippers", "Chilly", "Skipper", "Tux"],
    "Bear": ["Kodiak", "Honey"],
    "Kangaroo": ["Joey", "Roo", "Kanga", "Hopper", "Skip"],
    "Hippo": ["Hippo", "Fiona", "River"],
    "Rhinoceros": ["Rhino", "Spike"],
    "Cheetah": ["Speedy", "Cheeto", "Sprinter", "Spot"],
    "Chimpanzee": ["Chimpy", "Banana", "Bubbles", "Charlie", "Cheeky"],
    "Koala": ["Koda", "Eucalyptus", "Kylie", "Kookie", "Kozy"],
    "Flamingo": ["Flare", "Flossie", "Flamenco", "Pinky", "Sunny"],
    "Alligator": ["Ally", "Gator", "Snappy", "Swampy"],
    "Snake": ["Slither", "Slytherin", "Serpent", "Hiss", "Python"],
    "Lemur": ["Lemmy", "Luna", "Lenny", "Larry", "Lila"],
    "Panda": ["PanPan", "Bamboo"],
    "Eagle": ["Freedom", "Liberty"]
}

def output_file(file, events):
    with open(f'feeding_events_{file}.jsonl', 'w') as out_file:
        for event in events:
            out_file.write(event + "\n")

# generate feeding events for the next year
start = datetime.now()
events = []
file = 1
for day in range(0, 365):
    for event in generate_feeding_events(start + timedelta(days=day)):
        events.append(json.dumps(event))
        if len(events) == 5000:
                output_file(file, events)
                # increment file count and trip array leaving 500 items overlapping next file
                file += 1
                events = events[4500:]
output_file(file, events)
