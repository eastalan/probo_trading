import pyttsx3

def speak_event(event_text):
    """
    Uses system voice to read out the event text.
    """
    try:
        engine = pyttsx3.init()
        engine.say(event_text)
        engine.runAndWait()
    except Exception as e:
        print(f"Voice error: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        speak_event(" ".join(sys.argv[1:]))