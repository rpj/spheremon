#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <signal.h>

#include <applibs/networking.h>
#include <applibs/gpio.h>

#include <yarl.h>

// adapted from http://beej.us/guide/bgnet/html/multi/clientserver.html#simpleclient

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET)
    {
        return &(((struct sockaddr_in *)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

RedisConnection_t RedisConnect(const char *host, const char *port)
{
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0)
    {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    // loop through all the results and connect to the first we can
    for (p = servinfo; p != NULL; p = p->ai_next)
    {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            perror("client: socket");
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1)
        {
            close(sockfd);
            perror("client: connect");
            continue;
        }

        break;
    }

    if (p == NULL)
    {
        fprintf(stderr, "client: failed to connect\n");
        return -2;
    }

    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
    freeaddrinfo(servinfo); // all done with this structure
    return (RedisConnection_t)sockfd;
}

#define WAIT_FOR_WIFI_SECONDS   120

bool netCheck(void);

bool netCheck()
{
    bool netUp = false;
    return Networking_IsNetworkingReady(&netUp) != -1 && netUp;
}

int checkKeys(RedisConnection_t conn, RedisArray_t *keys)
{
    assert(keys && keys->count);
    int lostCount = 0;
    for (int i = 0; i < keys->count; i++)
    {
        char *checkKey = (char*)keys->objects[i].obj;
        lostCount += !Redis_EXISTS(conn, checkKey);
    }
    return lostCount;
}

#define RED_LED 8
#define GREEN_LED 9
#define BLUE_LED 10
#define RED_FDIDX (RED_LED-RED_LED)
#define GREEN_FDIDX (GREEN_LED-RED_LED)
#define BLUE_FDIDX (BLUE_LED-RED_LED)
#define LED_COUNT 3
#define LED_ON GPIO_Value_Low
#define LED_OFF GPIO_Value_High

#define LOST_LED RED_FDIDX
#define ACTIVITY_LED GREEN_FDIDX
#define LOST_PULSE_LED BLUE_FDIDX

#define KEY_CHECK_CADENCE_SECONDS 5
#define MSG_CADENCE_AMOUNT 10

int* setupLEDs(void);
int* setupLEDs()
{
    // keeps these in index order from above or else!
    int leds[LED_COUNT] = { RED_LED, GREEN_LED, BLUE_LED };
    int* fds = (int*)malloc(LED_COUNT * sizeof(int));

    for (int i = 0; i < LED_COUNT; i++) {
        fds[i] = GPIO_OpenAsOutput(leds[i], GPIO_OutputMode_PushPull, GPIO_Value_High);
        if (fds[i] < 0) {
            fprintf(stderr,
                "Error opening GPIO %u: %s (%d). Check that app_manifest.json includes the GPIO used.\n",
                leds[i], strerror(errno), errno);
            return NULL;
        }
    }
    printf("LEDs initialized: activity=%d lost=%d lost-count=%d\n", 
        ACTIVITY_LED+RED_LED, LOST_LED+RED_LED, LOST_PULSE_LED+RED_LED);
    return fds;
}

#define TOGGLE_ALL(fds, val) do { for (int i = 0; i < LED_COUNT; i++) GPIO_SetValue(fds[i], val); } while(0)

typedef struct psubThreadArgs
{
    int* fds;
    const char* host;
    const char* port;
    const char* pass;
} psubThreadArgs_t;

int trackedKeyCount = 0;
int __attribute__((atomic)) msgCount = 0;
int __attribute__((atomic)) lastLost = 0;
int __attribute__((atomic)) threadRunningCount = 0;
static volatile sig_atomic_t running = true;

RedisConnection_t newConnection(psubThreadArgs_t* tArgs)
{
    assert(tArgs);
    RedisConnection_t threadConn = RedisConnect(tArgs->host, tArgs->port);

    if (threadConn < 1)
    {
        fprintf(stderr, "RedisConnect failed: %d\n", threadConn);
        fflush(stderr);
        exit(42);
    }

    if (tArgs->pass && !Redis_AUTH(threadConn, tArgs->pass))
    {
        fprintf(stderr, "AUTH failed\n");
        fflush(stderr);
        exit(43);
    }

    return threadConn;
}

void* watchThreadFunc(void* arg)
{
    assert(arg);
    RedisConnection_t threadConn = newConnection((psubThreadArgs_t*)arg);
    static const double SLEEP_TIME_SECONDS = 5.0;
    printf("watch thread up and running.\n");
    threadRunningCount++;

    struct timespec sleepTime = { (time_t)SLEEP_TIME_SECONDS, 0 };
    int last = 0;
    double perSec = 0.0, curPerSec = 0.0;
    time_t timeIncr = 0;
    char buf[128];
    while (running)
    {
        if (!last) {
            perSec = msgCount / SLEEP_TIME_SECONDS;
        }
        else {
            curPerSec = (msgCount - last) / SLEEP_TIME_SECONDS;
            perSec = (curPerSec + perSec) / 2;
        }

        if (timeIncr) {
            bzero(buf, 128);
            snprintf(buf, 128, "[%06d] %-6d %-6d %-3d %5.2f %5.2f %s",
                timeIncr, msgCount, last, (msgCount - last), 
                perSec, curPerSec, (curPerSec > perSec * 1.5 ? "!>!" : (curPerSec < perSec * 0.5 ? "!<!" : "")));

            Redis_PUBLISH(threadConn, "spheremon:watchthread", buf);
#if DEBUG
            fprintf(stderr, "%s\n", buf);
            fflush(stderr);
#endif
        }

        last = msgCount;
        timeIncr += (time_t)SLEEP_TIME_SECONDS;
        nanosleep(&sleepTime, NULL);
    }

    printf("watch thread exiting.\n");
    --threadRunningCount;
}

void* psubThreadFunc(void* arg)
{
    assert(arg);
    psubThreadArgs_t* tArgs = (psubThreadArgs_t*)arg;
    RedisConnection_t threadConn = newConnection(tArgs);

    Redis_PSUBSCRIBE(threadConn, "*");
    printf("activity thread up and running.\n");
    threadRunningCount++;

    while (running)
    {
        RedisObject_t nextObj = RedisConnection_getNextObject(threadConn);
        RedisObject_dealloc(nextObj);

        if (!lastLost)
        {
            struct timespec quickTime = { 0, 1 };
            GPIO_SetValue(tArgs->fds[GREEN_FDIDX], LED_ON);
            nanosleep(&quickTime, NULL);
            GPIO_SetValue(tArgs->fds[GREEN_FDIDX], LED_OFF);
        }

        ++msgCount;
    }

    printf("activity thread exiting.\n");
    --threadRunningCount;
}

void* cmdThreadFunc(void* arg)
{
    assert(arg);
    psubThreadArgs_t* tArgs = (psubThreadArgs_t*)arg;
    RedisConnection_t threadConn = newConnection(tArgs);

    Redis_SUBSCRIBE(threadConn, "spheremon:command");
    printf("command thread up and running.\n");
    threadRunningCount++;

    while (running)
    {
        RedisObject_t nextObj = RedisConnection_getNextObject(threadConn);

        if (nextObj.type == RedisObjectType_Array && nextObj.obj)
        {
            RedisArray_t* arr = (RedisArray_t*)nextObj.obj;
            if (arr->count == 3 && arr->objects[2].type == RedisObjectType_BulkString && arr->objects[2].obj)
            {
                char sBuf[64];
                bzero(sBuf, 64);
                char* cmdStr = (char*)arr->objects[2].obj;
                bool sBufHasResp = true;

                if (!strncmp("message-count", cmdStr, strlen("message-count")))
                    snprintf(sBuf, 64, "%d", msgCount);
                else if (!strncmp("tracked-keys", cmdStr, strlen("tracked-keys")))
                    snprintf(sBuf, 64, "%d/%d", trackedKeyCount - lastLost, trackedKeyCount);
                else if (!strncmp("killkillkill", cmdStr, strlen("killkillkill")))
                {
                    printf("Kill command! Shutting down...\n");
                    fflush(stdout);
                    running = false;
                    sBufHasResp = false;
                }
                else
                    sBufHasResp = false;

                if (sBufHasResp)
                {
                    int chanNameLen = strlen("spheremon:command:result:") + strlen(cmdStr) + 1;
                    char* chanName = (char*)malloc(chanNameLen);
                    bzero(chanName, chanNameLen);
                    snprintf(chanName, chanNameLen, "spheremon:command:result:%s", cmdStr);

                    // can't use threadConn because it's in the "subscribe" modality
                    RedisConnection_t tempConn = newConnection(tArgs);
                    if (!Redis_SET(tempConn, chanName, sBuf))
                        fprintf(stderr, "failed to set %s\n", chanName);
                    Redis_PUBLISH(tempConn, chanName, sBuf);

                    close(tempConn);
                    free(chanName);

                    printf("Command '%s' respone: '%s'\n", cmdStr, sBuf);
                    fflush(stdout);
                }
            }
        }

        RedisObject_dealloc(nextObj);
    }

    printf("command thread exiting.\n");
    --threadRunningCount;
}

void sighand(int sig)
{
    if (sig == SIGTERM)
    {
        printf("Got SIGTERM! Shutting down...\n");
        running = false;
    }
}

int main(int argc, char** argv)
{
    signal(SIGTERM, sighand);

    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s host port password\n\n", argv[0]);
        exit(-1);
    }

    const char* host = argv[1];
    const char* port = argv[2];
    const char* pass = argc > 3 ? argv[3] : NULL;

    printf("Running GPIO setup for LEDs...\n");
    int* fds = setupLEDs();

    if (!fds)
    {
        fprintf(stderr, "LED setup failed\n\n");
        exit(-1);
    }

    TOGGLE_ALL(fds, LED_OFF);

    printf("Verifying network availability...\n");
    GPIO_SetValue(fds[BLUE_FDIDX], LED_ON);

    const struct timespec sleepTime = { 1, 0 };
    static int netCheckRetries = WAIT_FOR_WIFI_SECONDS;
    while (!netCheck() && --netCheckRetries)
    {
        const struct timespec quickTime = { 0, 5e7 };
        GPIO_SetValue(fds[RED_FDIDX], LED_ON);
        nanosleep(&quickTime, NULL);
        GPIO_SetValue(fds[RED_FDIDX], LED_OFF);
        nanosleep(&sleepTime, NULL);
    }

    GPIO_SetValue(fds[BLUE_FDIDX], LED_OFF);

    if (!netCheckRetries)
    {
        perror("Networking init failed");
        exit(-errno);
    }

    if (netCheckRetries != WAIT_FOR_WIFI_SECONDS)
    {
        printf("... waited %d seconds for network.\n", WAIT_FOR_WIFI_SECONDS - netCheckRetries);
    }

    printf("Connecting to redis://%s%s:%s...\n", pass ? "*@" : "", host, port);
    GPIO_SetValue(fds[GREEN_FDIDX], LED_ON);

    psubThreadArgs_t psubThreadArgs = {
        .fds = fds,
        .host = host,
        .port = port,
        .pass = pass
    };

    RedisConnection_t rConn = newConnection(&psubThreadArgs);

    printf("Querying expected key sets...\n");
    RedisArray_t *rpjiosKeys = Redis_KEYS(rConn, "rpjios.checkin.*");
    RedisArray_t *zerowatchKeys = Redis_KEYS(rConn, "*:heartbeat");

    if (!rpjiosKeys || !zerowatchKeys)
    {
        fprintf(stderr, "Failed to query key sets we expected\n");
        exit(-2);
    }

    const struct timespec loopTime = { KEY_CHECK_CADENCE_SECONDS, 0 };
    const struct timespec blinkTime = { 0, 5e8 };

    printf("Found %d rpjios and %d zerowatch keys to monitor every %ds\n",
        rpjiosKeys->count, zerowatchKeys->count, loopTime.tv_sec);
    trackedKeyCount = rpjiosKeys->count + zerowatchKeys->count;

    pthread_t psubThread;
    pthread_t commandThread;
    pthread_t watchThread;

    printf("Starting activity thread...\n");
    int pc = pthread_create(&psubThread, NULL, psubThreadFunc, &psubThreadArgs);

    if (pc)
    {
        fprintf(stderr, "pthread_create (activity): %d\n", pc);
        exit(pc);
    }

    printf("Starting command thread...\n");
    pc = pthread_create(&commandThread, NULL, cmdThreadFunc, &psubThreadArgs);

    if (pc)
    {
        fprintf(stderr, "pthread_create (command): %d\n", pc);
        exit(pc);
    }

    printf("Starting watch thread...\n");
    pc = pthread_create(&watchThread, NULL, watchThreadFunc, &psubThreadArgs);

    if (pc)
    {
        fprintf(stderr, "pthread_create (watch): %d\n", pc);
        exit(pc);
    }

    GPIO_SetValue(fds[BLUE_FDIDX], LED_OFF);
    while (threadRunningCount < 3);

    nanosleep(&blinkTime, NULL);
    TOGGLE_ALL(fds, LED_OFF);

    printf("spheremon fully initialized.\n");
    fflush(stdout);

    // printing to serial automatically lights the orange "App" LED, which from
    // here on out we'll use instead to mark every command-response messages, 
    // via the print statements emitted to serial from psubThreadFunc. nothing else should
    // print to serial, so as to allow that LED to function  command-response indicator

    while (running)
    {
        lastLost = checkKeys(rConn, rpjiosKeys) + checkKeys(rConn, zerowatchKeys);
        if (lastLost)
        {
            GPIO_SetValue(fds[LOST_LED], LED_ON);
            for (int i = 0; i < lastLost; i++)
            {
                GPIO_SetValue(fds[LOST_PULSE_LED], LED_ON);
                nanosleep(&blinkTime, NULL);
                GPIO_SetValue(fds[LOST_PULSE_LED], LED_OFF);
                nanosleep(&blinkTime, NULL);
            }
        }
        else
        {
            TOGGLE_ALL(fds, LED_OFF);
        }

        fflush(stdout);
        fflush(stderr);
        nanosleep(&loopTime, NULL);
    }

    printf("spheremon exiting (%d children left)...\n", threadRunningCount);
    pthread_join(psubThread, NULL);
    pthread_join(commandThread, NULL);
    printf("spheremon done, tracked %d total messages.\n", msgCount);
    fflush(stdout);
}