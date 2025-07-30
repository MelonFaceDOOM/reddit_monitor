from openai import OpenAI
from dotenv import load_dotenv
import os


load_dotenv()
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_CLIENT = OpenAI(api_key=OPENAI_API_KEY)
JOB_DESCRIPTION = open("analysis/acip/job_description.txt",
                       "r", encoding="utf-8").read()

"""
optional later:
 - new cgpt query to create sub-categories (eh...)
 - get comments and do sentiment? no point unlessi can actually think of how to get value out of it
     - the problem is it will just be a blob of both positive and negative submissions, and the comments being supportive or not doesn't necessarily say anything
"""


def single_prompt_response(prompt):
    """create a chatgpt instance, feed it one single prompt, and return the reply"""

    # temperature = get_temperature()
    # top_p = get_top_p()
    # presence_penalty = get_presence_penalty()
    # frequency_penalty = get_frequency_penalty()
    completion = OPENAI_CLIENT.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": JOB_DESCRIPTION},
            {"role": "user", "content": prompt}
        ]
    )
    response = completion.choices[0].message.content
    return response
