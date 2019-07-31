<b>Stats Predictor</b>

This program is used to make specific predictions for different statistics of NHL players for each NHL game in a season.
Currently, this program is optimized for the website FanDuel, but can be relatively easily adapted for other websites as well.
With some effort, other sports could also be included beyond just hockey, however this program was written primarily for the NHL.

Still a work in progress.

<b>WORKFLOW</b>

Ideally, the automatic daily workflow would consist of three distinct steps:
1. Gather data from online.
2. Use a neural network and machine learning to make prediction for the current day's players and games.
3. Use zero-one integer linear programming methods to compile the most optimized "lineup" for a website like FanDuel.

<b>Gathering Data</b>

The gathering of the data currently uses two separate methods.
- Retrieve the stats from the previous day's games from the NHL website API.
  - Simply uses the requests and json Python packages.
- Retrieve the restrictions for the current day's tournaments directly from the FanDuel website.
  - No easily-accessible website API exists for FanDuel, so this uses the Selenium WebDriver package to access the data.
  
The input for this step is the current date.

The output for this step is all of the necessary data that is gathered and placed into the database for use in future steps.

<b>Neural Network</b>

The main processing of data exists in this step.
As much data as possible is passed through an already-trained neural network.
Provides a prediction for applicable statistics for each player of each game of the current day.

The input for this step is the data retrieved from the previous step as well as the previously-trained neural network.

The output for this step is a prediction stored in the database for all requested statistics for each player for each game of the current date.

This step is still in development.

<b>Linear Programming</b>

The final step checks all of the predicted values from the previous step and provides the most optimized solution.

The input for this step is all of the predicted values from the Neural Network step as well as some information in the Gathering Data step.

The output for this step is an optimized "lineup" for a tournament on the FanDuel website.

<b>KNOWN ISSUES</b>

The biggest known issue currently is the difficulty of the last step.
There are too many inputs and not enough constraints to be able to narrow down the most optimized solution in a reasonable amount of time.

The Neural Network step is currently practically nonexistent as the prediction just uses the avarage of each statistic up to that point in the NHL season.

The connection to the database needs to be fixed so there is not as much hard-coding involved.

<b>FUTURE PLANS</b>

The plan is to complete the development of these three steps as much as possible using Python.
It is likely that some of this gets moved to a faster language like C. Especially the Linear Programming step.

Ideally, there should be some kind of GUI that a user can see the predictions and possibly even make small changes if necessary.
That GUI will likely be in the form of a Swift iOS application.

One a GUI is built, more customization can be done on the user's side to formulate a "lineup" that satisfies the user.
