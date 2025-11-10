#include "Samples.h"
#include "ami.h"
#include <gst/gst.h>
#include <gst/app/gstappsink.h>
#include <dirent.h>

#define env_path "/root/copilot/cfg/.aws_kvs_env"
#define encrypt_path "/root/copilot/dat/kvs"
#define encrypt_file_path "/root/copilot/dat/kvs/channel_name"

extern PSampleConfiguration gSampleConfiguration;

GstElement* senderPipeline = NULL;

GstFlowReturn on_new_sample(GstElement* sink, gpointer data, UINT64 trackid)
{
    GstBuffer* buffer;
    STATUS retStatus = STATUS_SUCCESS;
    BOOL isDroppable, delta;
    GstFlowReturn ret = GST_FLOW_OK;
    GstSample* sample = NULL;
    GstMapInfo info;
    GstSegment* segment;
    GstClockTime buf_pts;
    Frame frame;
    STATUS status;
    PSampleConfiguration pSampleConfiguration = (PSampleConfiguration) data;
    PSampleStreamingSession pSampleStreamingSession = NULL;
    PRtcRtpTransceiver pRtcRtpTransceiver = NULL;
    UINT32 i;
    guint bitrate;

    CHK_ERR(pSampleConfiguration != NULL, STATUS_NULL_ARG, "NULL sample configuration");

    info.data = NULL;
    sample = gst_app_sink_pull_sample(GST_APP_SINK(sink));

    buffer = gst_sample_get_buffer(sample);
    isDroppable = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_CORRUPTED) || GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DECODE_ONLY) ||
        (GST_BUFFER_FLAGS(buffer) == GST_BUFFER_FLAG_DISCONT) ||
        (GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DISCONT) && GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT)) ||
        // drop if buffer contains header only and has invalid timestamp
        !GST_BUFFER_PTS_IS_VALID(buffer);

    if (!isDroppable)
    {
        delta = GST_BUFFER_FLAG_IS_SET(buffer, GST_BUFFER_FLAG_DELTA_UNIT);

        frame.flags = delta ? FRAME_FLAG_NONE : FRAME_FLAG_KEY_FRAME;

        // convert from segment timestamp to running time in live mode.
        segment = gst_sample_get_segment(sample);
        buf_pts = gst_segment_to_running_time(segment, GST_FORMAT_TIME, buffer->pts);
        if (!GST_CLOCK_TIME_IS_VALID(buf_pts))
        {
            DLOGE("[KVS GStreamer Master] Frame contains invalid PTS dropping the frame");
        }

        if (!(gst_buffer_map(buffer, &info, GST_MAP_READ)))
        {
            DLOGE("[KVS GStreamer Master] on_new_sample(): Gst buffer mapping failed");
            goto CleanUp;
        }

        frame.trackId = trackid;
        frame.duration = 0;
        frame.version = FRAME_CURRENT_VERSION;
        frame.size = (UINT32) info.size;
        frame.frameData = (PBYTE) info.data;

        MUTEX_LOCK(pSampleConfiguration->streamingSessionListReadLock);
        for (i = 0; i < pSampleConfiguration->streamingSessionCount; ++i)
        {
            pSampleStreamingSession = pSampleConfiguration->sampleStreamingSessionList[i];
            frame.index = (UINT32) ATOMIC_INCREMENT(&pSampleStreamingSession->frameIndex);

            if (trackid == DEFAULT_AUDIO_TRACK_ID)
            {
                if (pSampleStreamingSession->pSampleConfiguration->enableTwcc && senderPipeline != NULL)
                {
                    GstElement* encoder = gst_bin_get_by_name(GST_BIN(senderPipeline), "sampleAudioEncoder");
                    if (encoder != NULL)
                    {
                        g_object_get(G_OBJECT(encoder), "bitrate", &bitrate, NULL);
                        MUTEX_LOCK(pSampleStreamingSession->twccMetadata.updateLock);
                        pSampleStreamingSession->twccMetadata.currentAudioBitrate = (UINT64) bitrate;
                        if (pSampleStreamingSession->twccMetadata.newAudioBitrate != 0)
                        {
                            bitrate = (guint) (pSampleStreamingSession->twccMetadata.newAudioBitrate);
                            pSampleStreamingSession->twccMetadata.newAudioBitrate = 0;
                            g_object_set(G_OBJECT(encoder), "bitrate", bitrate, NULL);
                        }
                        MUTEX_UNLOCK(pSampleStreamingSession->twccMetadata.updateLock);
                    }
                }
                pRtcRtpTransceiver = pSampleStreamingSession->pAudioRtcRtpTransceiver;
                frame.presentationTs = pSampleStreamingSession->audioTimestamp;
                frame.decodingTs = frame.presentationTs;
                pSampleStreamingSession->audioTimestamp +=
                    SAMPLE_AUDIO_FRAME_DURATION; // assume audio frame size is 20ms, which is default in opusenc
            } else
            {
                if (pSampleStreamingSession->pSampleConfiguration->enableTwcc && senderPipeline != NULL)
                {
                    GstElement* encoder = gst_bin_get_by_name(GST_BIN(senderPipeline), "sampleVideoEncoder");
                    if (encoder != NULL)
                    {
                        g_object_get(G_OBJECT(encoder), "bitrate", &bitrate, NULL);
                        MUTEX_LOCK(pSampleStreamingSession->twccMetadata.updateLock);
                        pSampleStreamingSession->twccMetadata.currentVideoBitrate = (UINT64) bitrate;
                        if (pSampleStreamingSession->twccMetadata.newVideoBitrate != 0)
                        {
                            bitrate = (guint) (pSampleStreamingSession->twccMetadata.newVideoBitrate);
                            pSampleStreamingSession->twccMetadata.newVideoBitrate = 0;
                            g_object_set(G_OBJECT(encoder), "bitrate", bitrate, NULL);
                        }
                        MUTEX_UNLOCK(pSampleStreamingSession->twccMetadata.updateLock);
                    }
                }
                pRtcRtpTransceiver = pSampleStreamingSession->pVideoRtcRtpTransceiver;
                frame.presentationTs = pSampleStreamingSession->videoTimestamp;
                frame.decodingTs = frame.presentationTs;
                pSampleStreamingSession->videoTimestamp += SAMPLE_VIDEO_FRAME_DURATION; // assume video fps is 25
            }
            status = writeFrame(pRtcRtpTransceiver, &frame);
            if (status != STATUS_SRTP_NOT_READY_YET && status != STATUS_SUCCESS)
            {
            }
            else if (status == STATUS_SUCCESS && pSampleStreamingSession->firstFrame)
            {
                PROFILE_WITH_START_TIME(pSampleStreamingSession->offerReceiveTime, "Time to first frame");
                pSampleStreamingSession->firstFrame = FALSE;
            }
            else if (status == STATUS_SRTP_NOT_READY_YET)
            {
                DLOGI("[KVS GStreamer Master] SRTP not ready yet, dropping frame");
            }
        }
        MUTEX_UNLOCK(pSampleConfiguration->streamingSessionListReadLock);
    }

CleanUp:

    if (info.data != NULL)
    {
        gst_buffer_unmap(buffer, &info);
    }

    if (sample != NULL)
    {
        gst_sample_unref(sample);
    }

    if (ATOMIC_LOAD_BOOL(&pSampleConfiguration->appTerminateFlag))
    {
        ret = GST_FLOW_EOS;
    }

    return ret;
}

GstFlowReturn on_new_sample_video(GstElement* sink, gpointer data)
{
    return on_new_sample(sink, data, DEFAULT_VIDEO_TRACK_ID);
}

PVOID sendGstreamerAudioVideo(PVOID args)
{
    STATUS retStatus = STATUS_SUCCESS;
    GstElement *appsinkVideo = NULL, *appsinkAudio = NULL;
    GstBus* bus;
    GstMessage* msg;
    GError* error = NULL;
    PSampleConfiguration pSampleConfiguration = (PSampleConfiguration) args;

    CHK_ERR(pSampleConfiguration != NULL, STATUS_NULL_ARG, "[KVS Gstreamer Master] Streaming session is NULL");

    CHAR rtspPipeLineBuffer[RTSP_PIPELINE_MAX_CHAR_COUNT];

    UINT16 stringOutcome = SNPRINTF(rtspPipeLineBuffer, RTSP_PIPELINE_MAX_CHAR_COUNT,
                                    "rtspsrc location=%s latency=0 ! "
                                    "rtph264depay ! h264parse config-interval=1 ! "
                                    "video/x-h264,stream-format=byte-stream,alignment=au ! "
                                    "queue ! appsink sync=TRUE emit-signals=TRUE name=appsink-video",
                                    pSampleConfiguration->rtspUri);

    if (stringOutcome > RTSP_PIPELINE_MAX_CHAR_COUNT)
    {
        DLOGE("[KVS GStreamer Master] ERROR: rtsp uri entered exceeds maximum allowed length set by RTSP_PIPELINE_MAX_CHAR_COUNT");
        goto CleanUp;
    }
    senderPipeline = gst_parse_launch(rtspPipeLineBuffer, &error);

    CHK_ERR(senderPipeline != NULL, STATUS_NULL_ARG, "[KVS Gstreamer Master] Pipeline is NULL");

    appsinkVideo = gst_bin_get_by_name(GST_BIN(senderPipeline), "appsink-video");

    g_signal_connect(appsinkVideo, "new-sample", G_CALLBACK(on_new_sample_video), (gpointer) pSampleConfiguration);

    gst_element_set_state(senderPipeline, GST_STATE_PLAYING);

    /* block until error or EOS */
    bus = gst_element_get_bus(senderPipeline);
    msg = gst_bus_timed_pop_filtered(bus, GST_CLOCK_TIME_NONE, GST_MESSAGE_ERROR | GST_MESSAGE_EOS);

    /* Free resources */
    if (msg != NULL)
    {
        gst_message_unref(msg);
    }
    if (bus != NULL)
    {
        gst_object_unref(bus);
    }
    if (senderPipeline != NULL)
    {
        gst_element_set_state(senderPipeline, GST_STATE_NULL);
        gst_object_unref(senderPipeline);
    }
    if (appsinkVideo != NULL)
    {
        gst_object_unref(appsinkVideo);
    }

CleanUp:

    if (error != NULL)
    {
        DLOGE("[KVS GStreamer Master] %s", error->message);
        g_clear_error(&error);
    }

    return (PVOID) (ULONG_PTR) retStatus;
}

char g_device_sn[INI_STRING_LEN];

int get_sn_from_shm_cfg(const char* p_sn, int sn_buf_sz)
{
    // 0. check args
    if (p_sn == NULL)
    {
        return -1;
    }
    if (sn_buf_sz < INI_STRING_LEN)
    {
        return -2;
    }

    //
    int shmidx = 0;
    int shmflag = 0;
    int shmid = 0;
    void* shm = NULL;

    T_SHM__CFG* pSHM_CFG = NULL;

    // 1. attach SHM_CFG
    shmidx = AMI_SHM_IDX__CFG;
    shmflag = 0444;
    shmid = shmget(SHM_KEY__CFG, SHM_SZ__CFG, shmflag);
    if (shmid < 0)
    {
        PR("[XXX] shmget(%d,%08x,%lu,%0o) fail. err=%d\n", AMI_SHM_IDX__CFG, SHM_KEY__CFG, SHM_SZ__CFG, shmflag, errno);
        return -11;
    }

    shm = (void*) shmat(shmid, (void*) 0, 0);
    if (shm == (void*) -1)
    {
        PR("[XXX] shmat(%d,%08x,%lu) fail. err=%d\n", shmidx, SHM_KEY__CFG, SHM_SZ__CFG, errno);
        return -12;
    }

    // 2. copy device_sn to 'p_sn'
    pSHM_CFG = (T_SHM__CFG*) shm;

    (void) memcpy(g_device_sn, pSHM_CFG->por_cfg.device_sn, INI_STRING_LEN);

    (void) memset(p_sn, 0, sn_buf_sz);
    (void) memcpy(p_sn, pSHM_CFG->por_cfg.device_sn, INI_STRING_LEN);

    // n.
    return 0;
}

void encrypt_serial(const char* serial, char* channel_name)
{
    uint64_t hash = 1469598103934665603ULL; // FNV-1a 64-bit offset basis
    const uint64_t prime = 1099511628211ULL;

    // FNV-1a hash
    for (size_t i = 0; i < strlen(serial); i++)
    {
        hash ^= (unsigned char) serial[i];
        hash *= prime;
    }

    const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    size_t charset_len = sizeof(charset) - 1;

    // Generate 8 characters from hash
    for (int i = 0; i < 14; i++)
    {
        channel_name[i] = charset[hash % charset_len];
        hash /= charset_len;
    }
    channel_name[14] = '\0';
}

void load_env_file(const char* path)
{
    FILE* fp = fopen(path, "r");
    if (!fp)
    {
        perror("Failed to open .env file");
        return;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp))
    {
        // Skip comments and empty lines
        if (line[0] == '#' || line[0] == '\n')
        {
            continue;
        }

        // Remove trailing newline
        line[strcspn(line, "\n")] = 0;

        // Split key=value
        char* eq = strchr(line, '=');
        if (!eq)
            continue;

        *eq = 0;
        char* key = line;
        char* value = eq + 1;

        // Optional: remove surrounding quotes
        if (*value == '\'' || *value == '"')
        {
            value++;
            value[strlen(value) - 1] = '\0';
        }

        // Export it to environment
        setenv(key, value, 1);
    }

    fclose(fp);
}

int is_process_running(const char* name)
{
    DIR* dir = opendir("/proc");
    if (!dir)
    {
        return -1;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_type != 4) // DT_DIR = 4, if directory
        {
            continue;
        }

        // Check if directory name is numeric (PID)
        int pid = atoi(entry->d_name);
        if (pid <= 0) {
            continue;
        }

        char path[256], cmdline[256];
        snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);

        FILE* f = fopen(path, "r");
        if (!f) {
            continue;
        }

        if (fgets(cmdline, sizeof(cmdline), f))
        {
            // cmdline is '\0' separated arguments
            if (strstr(cmdline, name) != NULL)
            {
                fclose(f);
                closedir(dir);
                return 0; // process found
            }
        }
        fclose(f);
    }

    closedir(dir);
    return -1; // process not found
}

INT32 main(INT32 argc, CHAR* argv[])
{
    load_env_file(env_path);

    STATUS retStatus = STATUS_SUCCESS;
    PSampleConfiguration pSampleConfiguration = NULL;
    PCHAR pChannelName;
    RTC_CODEC audioCodec = RTC_CODEC_OPUS;
    RTC_CODEC videoCodec = RTC_CODEC_H264_PROFILE_42E01F_LEVEL_ASYMMETRY_ALLOWED_PACKETIZATION_MODE;

    SET_INSTRUMENTED_ALLOCATORS();
    UINT32 logLevel = 7; // set silence

    signal(SIGINT, sigintHandler);

    /* Get SN of ORU */
    char device_sn[INI_STRING_LEN];
    get_sn_from_shm_cfg(device_sn, INI_STRING_LEN);
    // printf("SN %s\n",device_sn);
    /* Get SN of ORU */
    /* Convert to encrypted password */
    char channel_name_org[64] = "coras-cctv";
    char channel_name[64] = {0};
    encrypt_serial(device_sn, channel_name);
    printf("Channel name %s\n", channel_name); // this should be the channel name
    /* Convert to encrypted password */
    /* Apply channel name automatically */
    pChannelName = (char*) malloc(strlen(channel_name_org) + 1);
    strcpy(pChannelName, channel_name_org); // should be changed encrypted one
    printf("Channel name open %s\n", pChannelName);
    /* Apply channel name automatically */

    BOOL ready = FALSE;

    int ams_run = -1;
    int go2rtc_run = -1;

    while (!ready)
    {
        ams_run = is_process_running("ams");
        go2rtc_run = is_process_running("go2rtc");
        if (ams_run == 0 && go2rtc_run == 0)
        {
            ready = TRUE;
            break;
        }
        else
        {
            g_usleep(1000 * 1000); // 1000ms
        }
    }

    CHK_STATUS(createSampleConfiguration(pChannelName, SIGNALING_CHANNEL_ROLE_TYPE_MASTER, TRUE, TRUE, logLevel, &pSampleConfiguration));

    pSampleConfiguration->customData = (UINT64) pSampleConfiguration;
    pSampleConfiguration->srcType = RTSP_SOURCE;
    pSampleConfiguration->mediaType = SAMPLE_STREAMING_VIDEO_ONLY;
    pSampleConfiguration->audioCodec = audioCodec;
    pSampleConfiguration->videoCodec = videoCodec;

    char rtsp_address[64] = "rtsp://127.0.0.1:8588/camera0";
    pSampleConfiguration->rtspUri = (char*) malloc(strlen(rtsp_address) + 1);
    strcpy(pSampleConfiguration->rtspUri, rtsp_address);
    pSampleConfiguration->videoSource = sendGstreamerAudioVideo;

    // TO DO: Write encrypted channel name to dat folder
    /* Make kvs folder in dat */
    struct stat st = {0};
    if (stat(encrypt_path, &st) == -1)
    {
        if (mkdir(encrypt_path, 0755) == -1)
        {
        perror("Make dat folder fail");
        }
    }
    /* Make kvs folder in dat */
    /* Make kvs file in dat/kvs */
    FILE *fp = fopen(encrypt_file_path, "w");
    if (fp == NULL)
    {
        perror("Cannot open txt file\n");
    }
    fprintf(fp, "%s\n", pChannelName);

    fclose(fp);
    /* Make kvs file in dat/kvs */

    /* Initialize GStreamer */
    gst_init(&argc, &argv);
    // DLOGI("[KVS Gstreamer Master] Finished initializing GStreamer and handlers");

    // Initalize KVS WebRTC. This must be done before anything else, and must only be done once.
    CHK_STATUS(initKvsWebRtc());
    // DLOGI("[KVS GStreamer Master] KVS WebRTC initialization completed successfully");

    CHK_STATUS(initSignaling(pSampleConfiguration, SAMPLE_MASTER_CLIENT_ID));
    // DLOGI("[KVS GStreamer Master] Channel %s set up done ", pChannelName);

    // Checking for termination
    // CHK_STATUS(sessionCleanupWait(pSampleConfiguration));
    // DLOGI("[KVS GStreamer Master] Streaming session terminated");

    /* Clean termination */
    retStatus = STATUS_SUCCESS;
    PSampleStreamingSession pSampleStreamingSession = NULL;
    UINT32 i, clientIdHash;
    BOOL sampleConfigurationObjLockLocked = FALSE, streamingSessionListReadLockLocked = FALSE, peerConnectionFound = FALSE, sessionFreed = FALSE;
    SIGNALING_CLIENT_STATE signalingClientState;

    CHK(pSampleConfiguration != NULL, STATUS_NULL_ARG);
    while (ready)
    {
        ams_run = is_process_running("ams");
        go2rtc_run = is_process_running("go2rtc");
        if (ams_run != 0 || go2rtc_run != 0)
        {
            ready = FALSE;
            if (sampleConfigurationObjLockLocked)
            {
                MUTEX_UNLOCK(pSampleConfiguration->sampleConfigurationObjLock);
            }

            if (streamingSessionListReadLockLocked)
            {
                MUTEX_UNLOCK(pSampleConfiguration->streamingSessionListReadLock);
            }
            break;
        }
        else
        {
            MUTEX_LOCK(pSampleConfiguration->sampleConfigurationObjLock);
            sampleConfigurationObjLockLocked = TRUE;

            // scan and cleanup terminated streaming session
            for (i = 0; i < pSampleConfiguration->streamingSessionCount; ++i)
            {
                if (ATOMIC_LOAD_BOOL(&pSampleConfiguration->sampleStreamingSessionList[i]->terminateFlag))
                {
                    pSampleStreamingSession = pSampleConfiguration->sampleStreamingSessionList[i];

                    MUTEX_LOCK(pSampleConfiguration->streamingSessionListReadLock);
                    streamingSessionListReadLockLocked = TRUE;

                    // swap with last element and decrement count
                    pSampleConfiguration->streamingSessionCount--;
                    pSampleConfiguration->sampleStreamingSessionList[i] =
                        pSampleConfiguration->sampleStreamingSessionList[pSampleConfiguration->streamingSessionCount];

                    // Remove from the hash table
                    clientIdHash = COMPUTE_CRC32((PBYTE) pSampleStreamingSession->peerId, (UINT32) STRLEN(pSampleStreamingSession->peerId));
                    CHK_STATUS(hashTableContains(pSampleConfiguration->pRtcPeerConnectionForRemoteClient, clientIdHash, &peerConnectionFound));
                    if (peerConnectionFound)
                    {
                        CHK_STATUS(hashTableRemove(pSampleConfiguration->pRtcPeerConnectionForRemoteClient, clientIdHash));
                    }

                    MUTEX_UNLOCK(pSampleConfiguration->streamingSessionListReadLock);
                    streamingSessionListReadLockLocked = FALSE;

                    CHK_STATUS(freeSampleStreamingSession(&pSampleStreamingSession));
                    sessionFreed = TRUE;
                }
            }

            if (sessionFreed && pSampleConfiguration->channelInfo.useMediaStorage && !ATOMIC_LOAD_BOOL(&pSampleConfiguration->recreateSignalingClient))
            {
                // In the WebRTC Media Storage Ingestion Case the backend will terminate the session after
                // 1 hour.  The SDK needs to make a new JoinSession Call in order to receive a new
                // offer from the backend.  We will create a new sample streaming session upon receipt of the
                // offer.  The signalingClientConnectSync call will result in a JoinSession API call being made.
                CHK_STATUS(signalingClientDisconnectSync(pSampleConfiguration->signalingClientHandle));
                CHK_STATUS(signalingClientFetchSync(pSampleConfiguration->signalingClientHandle)); // contains while
                CHK_STATUS(signalingClientConnectSync(pSampleConfiguration->signalingClientHandle));
                sessionFreed = FALSE;
            }

            // Check if we need to re-create the signaling client on-the-fly
            if (ATOMIC_LOAD_BOOL(&pSampleConfiguration->recreateSignalingClient))
            {
                retStatus = signalingClientFetchSync(pSampleConfiguration->signalingClientHandle); // contains while
                if (STATUS_SUCCEEDED(retStatus))
                {
                    // Re-set the variable again
                    ATOMIC_STORE_BOOL(&pSampleConfiguration->recreateSignalingClient, FALSE);
                }
                else if (signalingCallFailed(retStatus))
                {
                    // printf("[KVS Common] recreating Signaling Client\n");
                    freeSignalingClient(&pSampleConfiguration->signalingClientHandle);
                    createSignalingClientSync(&pSampleConfiguration->clientInfo, &pSampleConfiguration->channelInfo,
                                              &pSampleConfiguration->signalingClientCallbacks, pSampleConfiguration->pCredentialProvider,
                                              &pSampleConfiguration->signalingClientHandle); // contains while
                }
            }

            // Check the signaling client state and connect if needed
            if (IS_VALID_SIGNALING_CLIENT_HANDLE(pSampleConfiguration->signalingClientHandle))
            {
                CHK_STATUS(signalingClientGetCurrentState(pSampleConfiguration->signalingClientHandle, &signalingClientState));
                if (signalingClientState == SIGNALING_CLIENT_STATE_READY)
                {
                    UNUSED_PARAM(signalingClientConnectSync(pSampleConfiguration->signalingClientHandle));
                }
            }

            // Check if any lingering pending message queues
            CHK_STATUS(removeExpiredMessageQueues(pSampleConfiguration->pPendingSignalingMessageForRemoteClient));

            // periodically wake up and clean up terminated streaming session
            CVAR_WAIT(pSampleConfiguration->cvar, pSampleConfiguration->sampleConfigurationObjLock, SAMPLE_SESSION_CLEANUP_WAIT_PERIOD);
            MUTEX_UNLOCK(pSampleConfiguration->sampleConfigurationObjLock);
            sampleConfigurationObjLockLocked = FALSE;
            // g_usleep(100 * 1000); // 100ms
        }
    }

    if (ready == FALSE)
    {
        free(pChannelName);
        free(pSampleConfiguration->rtspUri);
        goto CleanUp;
    }
    /* Clean termination */

CleanUp:

    if (retStatus != STATUS_SUCCESS)
    {
        DLOGE("[KVS GStreamer Master] Terminated with status code 0x%08x", retStatus);
    }

    DLOGI("[KVS GStreamer Master] Cleaning up....");

    if (pSampleConfiguration != NULL)
    {
        // Kick of the termination sequence
        ATOMIC_STORE_BOOL(&pSampleConfiguration->appTerminateFlag, TRUE);

        if (pSampleConfiguration->mediaSenderTid != INVALID_TID_VALUE)
        {
            THREAD_JOIN(pSampleConfiguration->mediaSenderTid, NULL);
        }

        // if (pSampleConfiguration->enableFileLogging) {
        //     freeFileLogger();
        // }
        retStatus = freeSignalingClient(&pSampleConfiguration->signalingClientHandle);
        if (retStatus != STATUS_SUCCESS)
        {
            DLOGE("[KVS GStreamer Master] freeSignalingClient(): operation returned status code: 0x%08x", retStatus);
        }

        retStatus = freeSampleConfiguration(&pSampleConfiguration);
        if (retStatus != STATUS_SUCCESS)
        {
            DLOGE("[KVS GStreamer Master] freeSampleConfiguration(): operation returned status code: 0x%08x", retStatus);
        }
    }
    DLOGI("[KVS Gstreamer Master] Cleanup done");

    RESET_INSTRUMENTED_ALLOCATORS();
    return STATUS_FAILED(retStatus) ? EXIT_FAILURE : EXIT_SUCCESS;
}