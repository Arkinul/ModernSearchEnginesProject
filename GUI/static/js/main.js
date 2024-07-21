document.addEventListener('DOMContentLoaded', () => {
    const searchBar = document.getElementById('searchBar');
    const wordCloudContainer = document.getElementById('wordCloudContainer');
    const container = document.querySelector('.container');
    const closeButton = document.getElementById('closeButton');

    searchBar.addEventListener('input', () => {
        if (searchBar.value) {
            closeButton.style.display = 'block';
        } else {
            closeButton.style.display = 'none';
        }
    });

    searchBar.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            const query = searchBar.value.trim();
            if (query) {
                fetchWordCloud(query);
                container.classList.add('moved-up');
                fadeIn(wordCloudContainer);
            }
        }
    });

    closeButton.addEventListener('click', () => {
        searchBar.value = '';
        closeButton.style.display = 'none';
        fadeOut(wordCloudContainer, () => {
            wordCloudContainer.classList.add('hidden');
            wordCloudContainer.innerHTML = '';
            container.classList.remove('moved-up');
        });
    });

    function fetchWordCloud(query) {
        fetch('/generate_word_cloud', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query: query }),
        })
        .then(response => response.json())
        .then(results => {
            generateWordCloud(results);
        })
        .catch(error => {
            console.error('Error fetching word cloud:', error);
        });
    }

    function generateWordCloud(results) {
        wordCloudContainer.innerHTML = '';

        const maxBubbleSize = 150;
        const minBubbleSize = 50;

        results.forEach((word) => {
            const wordElement = document.createElement('span');
            wordElement.textContent = word.text;
            wordElement.classList.add('bubble');
            wordCloudContainer.appendChild(wordElement);

            const size = (word.value / 100) * (maxBubbleSize - minBubbleSize) + minBubbleSize;
            wordElement.style.width = `${size}px`;
            wordElement.style.height = `${size}px`;

            const fontSize = size * 0.2;
            wordElement.style.fontSize = `${fontSize}px`;

            adjustFontSizeToFit(wordElement, size);

            // Random delay to each word's appearance
            const randomDelay = Math.random() * 2000;
            setTimeout(() => {
                wordElement.style.opacity = 1;
                wordElement.style.transform = 'scale(1)';
            }, randomDelay);
        });
    }

    function adjustFontSizeToFit(element, size) {
        const maxFontSize = size * 0.2;
        let fontSize = maxFontSize;
        element.style.fontSize = `${fontSize}px`;
        while (element.scrollWidth > element.clientWidth || element.scrollHeight > element.clientHeight) {
            fontSize -= 1;
            element.style.fontSize = `${fontSize}px`;
            if (fontSize <= 10) break;
        }
    }

    function fadeOut(element, callback) {
        element.style.opacity = 1;

        (function fade() {
            if ((element.style.opacity -= 0.1) < 0) {
                element.style.display = "none";
                if (callback) callback();
            } else {
                requestAnimationFrame(fade);
            }
        })();
    }

    function fadeIn(element) {
        element.style.opacity = 0;
        element.style.display = "flex";

        (function fade() {
            let val = parseFloat(element.style.opacity);
            if (!((val += 0.1) > 1)) {
                element.style.opacity = val;
                requestAnimationFrame(fade);
            }
        })();
    }
});
