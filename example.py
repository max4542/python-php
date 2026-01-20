
import hashlib
import json
from datetime import datetime
from urllib.parse import urlparse
from pusher_push_notifications import PushNotifications
import requests
from jwt import encode as jwt_encode
import secrets
import csv
from argon2 import PasswordHasher

# Secret key for JWT token encoding and decoding
SECRET_KEY = secrets.token_hex(32)

# Password hasher instance for secure password hashing and verification
password_hasher = PasswordHasher()

# User credentials (username, hashed password, and phone number)
user_credentials = {
    # 'password'
    'maxrai788@gmail.com': (
        '$argon2id$v=19$m=65536,t=3,p=4$E4EJ4eC4K1UjpiZzGj1JOg$T+MBp51a+0BBeEaiw2CXs3sVqUl/98MSRy6GoS2LBrE',
        '9807374556'
    ),
    # 'test'
    'user2@example.com': (
        '$argon2id$v=19$m=65536,t=3,p=4$rOclVtHIfRNrCwXRjq8Vog$ZLH+7wLCkC50x6TElJ5OmqZpGEdwrWgl92BfeYFNscg',
        '+1987654321'
    ),
}

class Blockchain:
    def __init__(self):
        self.chain = []
        self.transactions = []
        self.nodes = set()
        self.create_block(0, '0')
        self.INSTANCE_ID="c5d6a1a3-74a9-41c6-b4e9-d98bcb4ca740"
        self.SECRET_KEY="AB57A02AD0F229D4906D65D8BCE99B6802CEAA8382436DBA963FAC0B18A23FC3"
        self.connection = None

    def connect_to_pusher(self):
        try:
            self.connection = PushNotifications(
                instance_id=self.INSTANCE_ID,
                secret_key=self.SECRET_KEY
            )
            return True
        except PushNotifications.connector.Error as e:
            print("Error connecting to Pusher:", e)
            return False

    def create_block(self, nonce, previous_hash):
        block = {
            'index': len(self.chain) + 1,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'transactions': self.transactions,
            'nonce': nonce,
            'previous_hash': previous_hash
        }
        self.transactions = []
        self.chain.append(block)
        return block

    def get_previous_block(self):
        return self.chain[-1]

    def proof_of_work(self, previous_nonce):
        nonce = 0
        while self.valid_proof(previous_nonce, nonce) is False:
            nonce += 1
        return nonce

    def valid_proof(self, previous_nonce, nonce):
        guess = f'{previous_nonce}{nonce}'.encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        return guess_hash[:4] == '0000'  # Difficulty level, adjust as needed

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    def authenticate_user(self, email, password):
        # Check if user exists and password matches the stored Argon2 hash
        if email not in user_credentials:
        stored_hash = user_credentials[email][0]
        try:
            return password_hasher.verify(stored_hash, password)
        except Exception:
            return False
            return False

    def add_transaction(self):
        # Prompt user for email and password to authenticate
        email = input("Enter your email: ")
        password = input("Enter your password: ")
        if self.authenticate_user(email, password):
            sender = input("Enter sender: ")
            receiver = input("Enter receiver: ")
            amount = input("Enter amount: ")
            
            # Generate JWT token based on sender and receiver
            token_payload = {'sender': sender, 'receiver': receiver, 'amount': amount}
            jwt_token = jwt_encode(token_payload, SECRET_KEY, algorithm='HS256')
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            transaction = {'sender': sender, 'receiver': receiver, 'amount': amount,'jwt_token':jwt_token, 'timestamp': timestamp}
            self.transactions.append(transaction)
            # Log transaction to CSV file
            self.log_transaction_to_csv(transaction)
            
            # Send notification to the user's phone number
            user_phone_number = user_credentials[email][1]
            self.send_notification_to_phone(user_phone_number, "Transaction Notification", "Your transaction has been successfully added to the blockchain.")

            return True
        else:
            print("Authentication failed.")
            return False
        
    def log_transaction_to_csv(self, transaction):
        sender = transaction['sender']
        csv_file = f'D:/task/{sender}.csv'
        fieldnames = ['sender', 'receiver', 'amount','jwt_token', 'timestamp']
        with open(csv_file, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if file.tell() == 0:  # Check if file is empty and write headers if needed
                writer.writeheader()
            writer.writerow(transaction)

    def add_node(self, address):
        parsed_url = urlparse(address)
        self.nodes.add(parsed_url.netloc)

    def valid_chain(self, chain):
        previous_block = chain[0]
        block_index = 1
        while block_index < len(chain):
            block = chain[block_index]
            if block['previous_hash'] != self.hash(previous_block):
                return False
            previous_nonce = previous_block['nonce']
            nonce = block['nonce']
            if not self.valid_proof(previous_nonce, nonce):
                return False
            previous_block = block
            block_index += 1
        return True

    def resolve_conflicts(self):
        neighbours = self.nodes
        new_chain = None
        max_length = len(self.chain)

        for node in neighbours:
            response = requests.get(f'http://{node}/chain')
            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']
                if length > max_length and self.valid_chain(chain):
                    max_length = length
                    new_chain = chain

        if new_chain:
            self.chain = new_chain
            return True

        return False
    
    def send_notification_to_phone(self, phone_number, title, message):
        formatted_phone_number = phone_number.lstrip('+')
        if not self.connection:
            if not self.connect_to_pusher():
                print("Failed to connect to Pusher. Notification not sent.")
                return
        response = self.connection.publish(
            interests=[formatted_phone_number],
            publish_body={
                'fcm': {
                    'notification': {
                        'title': title,
                        'body': message
                    }
                }
            }
        )
        print("Notification sent successfully. Publish ID:", response['publishId'])

if __name__ == '__main__':
    blockchain = Blockchain()

    if blockchain.add_transaction():
        print("Transaction added successfully.")
        print("Mining block...")
        previous_block = blockchain.get_previous_block()
        previous_nonce = previous_block['nonce']
        nonce = blockchain.proof_of_work(previous_nonce)
        previous_hash = blockchain.hash(previous_block)
        block = blockchain.create_block(nonce, previous_hash)
        print("Block mined.")
        print("Current blockchain:")
        print(blockchain.chain)
    else:
        print("Invalid credentials")
