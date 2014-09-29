// Curl connection Header
//
// K Labe, September 23 2014
// K Labe, September 24 2014 - remove variables to source file
// K Labe, Setpember 29 2014 - add enum of types and function for it

enum alarm_type {DEBUG, INFO, SUCCESS, WARNING, ERROR};

// This function returns the alarm_type corresponding to the logging level
// number
alarm_type type(const int level);

// This opens the connection to minard
void Opencurl(char* password);

// This closes the connection to minard
void Closecurl();

// This function is used to send an alarm or log a message
// level sets the alarm type (see minard documentation)
// msg is the accompanying message (include &notify to alarm)
void alarm(const int level, const char* msg);
