STEPS TO RUN:
1. Download as zip file / clone the repository into local
2. Open terminal or bash in the directory's location and run "docker-compose build" - this is to get the container built
3. Next run "docker-compose up" to start the container
4. Open the Jupyter environment in your browser from the server link created once the previous command is run
5. Open terminal in the Jupyter environment and run "python main.py" - this will run the data pipeline code and create the datamart
6. Once done, go back to the terminal and run "docker-compose down" to stop the docker container running
