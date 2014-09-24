// Curl connection Header
//
// K Labe, September 23 2014
// K Labe, September 24 2014 - remove variables to source file

// This opens the connection to minard
void Opencurl(char* password);

// This closes the connection to minard
void Closecurl();

// This function is used to send an alarm or log a message
// level sets the alarm type (see minard documentation)
// msg is the accompanying message (include &notify to alarm)
void alarm(const int level, const char* msg);
