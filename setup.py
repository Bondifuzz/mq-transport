from setuptools import setup, find_packages


def parse_requirements(filename):
    result = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in (line.strip() for line in f):
            if not line or line.startswith("#"):
                continue

            if line.startswith("-r"):
                _, filename = line.split(" ", 1)
                result.extend(parse_requirements(filename))
            else:
                result.append(line)

    return result



with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="mq-transport",
    version="0.3.3",
    author="Pavel Knyazev",
    author_email="poulix.nova@mail.ru",
    url="https://github.com/Bondifuzz/mq-transport",
    description="Message queue based interface for communications in event-driven systems",
    install_requires=parse_requirements("requirements-base.txt"),
    extras_require={
        "sqs": parse_requirements("requirements-sqs.txt"),
    },
    packages=find_packages(exclude=["*tests*"]),
    long_description_content_type="text/markdown",
    long_description=long_description,
    python_requires=">=3.7",
)
