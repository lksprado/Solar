# Use an official Python runtime as a parent image
FROM python:3.12

# Set the working directory in the container
WORKDIR /usr/src/app

# Install any needed packages specified in requirements.txt or pyproject.toml
RUN pip install --no-cache-dir poetry
RUN poetry install --no-dev

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Run app.py when the container launches
CMD ["python", "your_main_script.py"]
