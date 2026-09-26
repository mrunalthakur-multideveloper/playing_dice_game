# Specify the base Docker image. You can read more about
# the available images at https://sdk.apify.com/docs/guides/docker-images
# You can also use any other image that has Python 3 installed.
FROM apify/actor-python-playwright:3.11

# Copy the requirements.txt file to the image
COPY requirements.txt .

# Install the dependencies in the requirements.txt file
RUN pip install -r requirements.txt

# Copy the rest of the source code to the image
COPY . .

# Run the actor
CMD ["python3", "main.py"]
