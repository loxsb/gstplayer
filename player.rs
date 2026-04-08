use crate::hub::hub_inst;
use crate::player::Command as PlayerCommand;
use crate::{hub, player::MediaInfo, player::MsgError};
use core::ffi::c_void;
use futures::stream::StreamExt;
use glib::object::{Cast, ObjectExt};
use gst::ClockTime;
use gst::{
    Bin, Element, ElementFactory, GhostPad, Message, Pad, PadProbeReturn, PadProbeType, Pipeline,
    State,
    prelude::{ElementExt, ElementExtManual, GstBinExtManual, PadExt, PadExtManual},
};
use gst_play::prelude::PlayStreamInfoExt;
use gst_play::{Play, PlayMessage, PlayState, PlayVideoRenderer};
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;

struct Branch {
    teepad: Pad,
    queue: Element,
    sink: Element,
}

pub(crate) struct Player {
    inner: Arc<PlayerInner>,
    cancel_token: CancellationToken,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

struct PlayerInner {
    play: Play,
    pipeline: Pipeline,
    video_handler: VideoHandler,
    audio_handler: AudioHandler,

    props: Arc<PlayProperty>,
}

struct VideoHandler {
    bin: Bin,
    tee: Element,
    branchs: std::sync::Mutex<HashMap<u32, Branch>>,
    next_id: std::sync::atomic::AtomicU32,
}

struct AudioHandler {
    sink: Element,
    filterbin: Bin,
    karaoke: Element,
    pitch: Element,
}
struct PlayProperty {
    state: std::sync::Mutex<PlayState>,
    loop_: std::sync::atomic::AtomicBool,
}

impl Player {
    pub fn new() -> Self {
        let inner = Arc::new(PlayerInner::new());
        let inner_clone = inner.clone();
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let join_handle = hub_inst().rt.spawn(async move {
            let mut messages = inner_clone.play.message_bus().stream().fuse();
            let mut cmd_rx = hub_inst().subscribe_player_command().await;
            loop {
                tokio::select! {
                    _ = cancel_token_clone.cancelled() => {
                        break;
                    }
                    maybe_msg = messages.next() => {
                        match maybe_msg {
                            Some(msg) => inner_clone.handle_message(msg),
                            None => {},
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Ok(command) => inner_clone.handle_command(command).await,
                            Err(_) => {}
                        }
                    }
                }
            }
        });

        Self {
            inner,
            cancel_token,
            join_handle: Some(join_handle),
        }
    }

    pub fn play(&self) {
        self.inner.play();
    }

    pub fn pause(&self) {
        self.inner.pause();
    }

    pub fn stop(&self) {
        self.inner.stop();
        // TODO 等待完全停止
    }

    pub fn add_branch(&self, item: *mut c_void) -> Result<u32, String> {
        self.inner.add_branch(glib::Value::from(item))
    }

    pub fn remove_branch(&self, id: u32) -> Result<(), String> {
        self.inner.remove_branch(id)
    }

    pub fn seek(&self, nseconds: u64) {
        self.inner.seek(nseconds);
    }

    pub fn set_uri(&self, uri: &str) {
        self.stop();
        self.inner.set_uri(uri);
    }
}

impl PlayerInner {
    pub fn new() -> Self {
        let video_handler = Self::build_video_sink().unwrap();
        let audio_handler = Self::build_audio_filter_sink().unwrap();

        let play = Play::new(None::<PlayVideoRenderer>);
        let pipeline = play.pipeline().downcast::<Pipeline>().unwrap();

        let video_sink_bin = video_handler.bin.clone();
        pipeline.set_property("video-sink", &video_sink_bin);
        pipeline.set_property("audio-sink", &audio_handler.sink);
        pipeline.set_property("audio-filter", &audio_handler.filterbin);

        let props = Arc::new(PlayProperty {
            state: std::sync::Mutex::new(PlayState::Stopped),
            loop_: std::sync::atomic::AtomicBool::new(false),
        });

        Self {
            play,
            pipeline,
            video_handler,
            audio_handler,
            props,
        }
    }

    fn build_audio_filter_sink() -> Result<AudioHandler, String> {
        let sink = gst::ElementFactory::make("autoaudiosink")
            .build()
            .map_err(|_| format!("make autoaudiosink failed"))?;

        let karaoke = gst::ElementFactory::make("audiokaraoke")
            .property("level", 0.0f32)
            .build()
            .map_err(|_| format!("make audiokaraoke failed"))?;

        let pitch = gst::ElementFactory::make("pitch")
            .property("pitch", 1.0f32)
            .build()
            .map_err(|_| format!("make pitch failed"))?;

        let bin = Bin::with_name("audiofilter");
        bin.add_many(&[&karaoke, &pitch])
            .map_err(|_| format!("Failed to add elements to audiofilter bin"))?;
        karaoke
            .link(&pitch)
            .map_err(|_| format!("failed to link pitch"))?;

        let sink_pad = karaoke
            .static_pad("sink")
            .ok_or_else(|| format!("Failed to get karaoke sink pad"))?;
        let ghost_sink_pad = gst::GhostPad::builder_with_target(&sink_pad)
            .unwrap()
            .build();
        ghost_sink_pad
            .set_active(true)
            .map_err(|_| format!("Failed to activate ghost pad"))?;

        bin.add_pad(&ghost_sink_pad)
            .map_err(|_| format!("Failed to add ghost pad to bin"))?;

        let src_pad = pitch.static_pad("src").expect("pitch has no src pad");
        let ghost_src_pad = gst::GhostPad::builder_with_target(&src_pad)
            .unwrap()
            .build();
        ghost_src_pad
            .set_active(true)
            .map_err(|_| format!("Failed to activate ghost pad"))?;
        bin.add_pad(&ghost_src_pad)
            .map_err(|_| format!("Failed to add ghost pad to bin"))?;

        Ok(AudioHandler {
            sink,
            filterbin: bin,
            karaoke,
            pitch,
        })
    }

    fn build_video_sink() -> Result<VideoHandler, String> {
        let bin = Bin::with_name("qmlsinkbin");

        let glupload = ElementFactory::make("glupload")
            .build()
            .map_err(|_| format!("make glupload failed"))?;
        let glcolorconvert = ElementFactory::make("glcolorconvert")
            .build()
            .map_err(|_| format!("make glcolorconvert failed"))?;
        let tee = ElementFactory::make("tee")
            .name("tee")
            // .property("allow-not-linked", true)
            .build()
            .map_err(|_| format!("make tee failed"))?;

        bin.add_many(&[&glupload, &glcolorconvert, &tee])
            .map_err(|_| format!("add elements to bin failed"))?;
        Element::link_many(&[&glupload, &glcolorconvert, &tee])
            .map_err(|_| format!("Failed to link tee to glcolorconvert"))?;

        let tee_sink_pad = tee
            .static_pad("sink")
            .ok_or_else(|| format!("Failed to get tee sink pad"))?;

        let ghost_pad = GhostPad::with_target(&tee_sink_pad)
            .map_err(|_| format!("Failed to create ghost pad"))?;
        ghost_pad
            .set_active(true)
            .map_err(|_| format!("Failed to activate ghost pad"))?;
        bin.add_pad(&ghost_pad)
            .map_err(|_| format!("Failed to add ghost pad to bin"))?;

        Ok(VideoHandler {
            bin,
            tee,
            branchs: std::sync::Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU32::new(1),
        })
    }

    pub fn play(&self) {
        self.play.play();
    }

    pub fn pause(&self) {
        self.play.pause();
    }

    pub fn stop(&self) {
        self.play.stop();
    }

    pub fn seek(&self, nseconds: u64) {
        self.play.seek(gst::ClockTime::from_nseconds(nseconds));
    }

    pub fn set_uri(&self, uri: &str) {
        self.stop();
        self.play
            .set_uri(uri.is_empty().then(|| None).unwrap_or(Some(uri)));
    }

    fn add_branch(&self, item: glib::Value) -> Result<u32, String> {
        let queue = ElementFactory::make("queue")
            .property("flush-on-eos", true)
            .build()
            .map_err(|_| format!("make queue failed"))?;

        let sink = ElementFactory::make("qml6glsink")
            .property("widget", item)
            // true 暂停时添加会等待prepoll卡住
            .property("async", false)
            .build()
            .map_err(|_| format!("make qml6glsink failed"))?;

        self.video_handler
            .bin
            .add_many(&[&queue, &sink])
            .map_err(|_| format!("Failed to add bin to pipeline"))?;

        queue
            .link(&sink)
            .map_err(|_| format!("Failed to link queue to sink"))?;

        let _ = queue.sync_state_with_parent();
        let _ = sink.sync_state_with_parent();

        let teepad = self
            .video_handler
            .tee
            .request_pad_simple("src_%u")
            .ok_or_else(|| format!("Failed to request tee src pad"))?;
        let sinkpad = queue
            .static_pad("sink")
            .ok_or_else(|| format!("Failed to get queue sink pad"))?;
        teepad
            .link(&sinkpad)
            .map_err(|e| format!("Failed to link tee src pad to queue sink pad: {}", e))?;

        let branch = Branch {
            teepad,
            queue,
            sink,
        };

        let id = self
            .video_handler
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        self.video_handler
            .branchs
            .lock()
            .unwrap()
            .insert(id, branch);

        Ok(id)
    }

    fn remove_branch(&self, id: u32) -> Result<(), String> {
        let branch = self
            .video_handler
            .branchs
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or_else(|| format!("Branch with id {} not found", id))?;

        let bin = self.video_handler.bin.clone();
        let tee = self.video_handler.tee.clone();
        let teepad = branch.teepad.clone();
        branch.sink.set_property("widget", None::<&glib::Object>);

        teepad.add_probe(PadProbeType::IDLE, move |pad, _| {
            if let Some(sinkpad) = pad.peer() {
                if let Err(e) = pad.unlink(&sinkpad) {
                    log::error!("Failed to unlink tee src pad from queue sink pad: {}", e);
                }
            }

            bin.remove_many([&branch.queue, &branch.sink]).unwrap();
            branch.queue.set_state(State::Null).unwrap();
            branch.sink.set_state(State::Null).unwrap();

            tee.release_request_pad(pad);
            PadProbeReturn::Remove
        });
        Ok(())
    }

    fn handle_message(&self, msg: Message) {
        use crate::hub::hub_inst;
        use crate::player::Message as Msg;

        match PlayMessage::parse(&msg) {
            Ok(PlayMessage::UriLoaded(uri)) => {
                hub_inst().send_player_message(Msg::Uri(uri.uri().to_string()));
            }

            Ok(PlayMessage::PositionUpdated(pos)) => {
                hub_inst().send_player_message(Msg::Position(
                    pos.position().unwrap_or_default().nseconds(),
                ));
            }

            Ok(PlayMessage::DurationChanged(duration)) => {
                hub_inst().send_player_message(Msg::Duration(
                    duration.duration().unwrap_or_default().nseconds(),
                ));
            }

            Ok(PlayMessage::StateChanged(state)) => {
                *self.props.state.lock().unwrap() = state.state();
                hub_inst().send_player_message(Msg::State(state.state()));
            }

            Ok(PlayMessage::VolumeChanged(volume)) => {
                hub_inst().send_player_message(Msg::Volume(volume.volume()));
            }

            Ok(PlayMessage::MediaInfoUpdated(info)) => {
                if let Some(video_stream) = info.media_info().video_streams().first() {
                    let _ = self
                        .play
                        .set_video_track_id(Some(video_stream.stream_id().as_str()));
                }
                hub_inst().send_player_message(Msg::MediaInfo(MediaInfo::from_gstmediainfo(
                    info.media_info(),
                )));
            }

            Ok(PlayMessage::Buffering(buf)) => {
                hub_inst().send_player_message(Msg::Buffering(buf.percent()));
            }

            Ok(PlayMessage::EndOfStream(_)) => {
                if self.props.loop_.load(std::sync::atomic::Ordering::SeqCst) {
                    let _ = self.play.seek(gst::ClockTime::ZERO);
                    self.play.play();
                }
                hub_inst().send_player_message(Msg::Eos);
            }

            Ok(PlayMessage::Error(msg)) => {
                hub_inst().send_player_message(Msg::Error(MsgError {
                    code: msg.error().code(),
                    message: msg
                        .details()
                        .map(|d| d.to_string())
                        .unwrap_or_else(|| "Unknown error".to_string()),
                }));
            }

            Ok(_) => (),
            Err(_) => unreachable!(),
        }
    }

    async fn handle_command(&self, cmd: PlayerCommand) {
        use crate::hub::hub_inst;
        use crate::player::Message as Msg;
        match cmd {
            PlayerCommand::PlayUri(play_uri) => {
                // 先暂停
                self.play.stop();
                // 怎么等待完全停止呢？
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let _ = self.pipeline.state(ClockTime::from_mseconds(500));

                self.set_uri(&play_uri.uri);

                // 再开始播放
                self.play.play();

                tokio::time::sleep(std::time::Duration::from_millis(20)).await;

                // TODO 设置其他属性

                if let Ok(mut tx) = play_uri.tx.lock() {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(());
                    }
                }
            }

            PlayerCommand::Play => self.play(),

            PlayerCommand::Pause => self.pause(),

            PlayerCommand::Stop => self.stop(),

            PlayerCommand::Seek(pos) => self.seek(pos),

            PlayerCommand::SetVolume(volume) => self.play.set_volume(volume),

            PlayerCommand::SetPitch(pitch) => {
                self.audio_handler.pitch.set_property("pitch", pitch);
                hub_inst().send_player_message(Msg::Pitch(pitch));
            }

            PlayerCommand::SetAudioTrack(track) => {
                if let Some(media_info) = self.play.media_info() {
                    let audios = media_info.audio_streams();
                    if let Some(stream) = audios.iter().nth(track as usize) {
                        if let Ok(_) = self
                            .play
                            .set_audio_track_id(Some(stream.stream_id().as_str()))
                        {
                            self.play.set_audio_track_enabled(true);
                            hub_inst().send_player_message(Msg::AudioTrack(track));
                        }
                    }
                }
            }

            PlayerCommand::SetLooping(looping) => {
                self.props
                    .loop_
                    .store(looping, std::sync::atomic::Ordering::SeqCst);
                hub_inst().send_player_message(Msg::Loop(looping));
            }

            PlayerCommand::SetVoiceLevel(level) => {
                self.audio_handler.karaoke.set_property("level", level);
                hub_inst().send_player_message(Msg::VoiceLevel(level));
            }
        }
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        self.stop();
        self.cancel_token.cancel();
        self.inner.play.message_bus().set_flushing(true);
        if let Some(handle) = self.join_handle.take() {
            let _ = hub::hub_inst().rt.block_on(handle);
        }
    }
}
