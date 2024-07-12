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
                generateWordCloud(query);
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

    function generateWordCloud(query) {
        // Search result data
        const results = [
            { text: 'Tübingen Concerts', value: 100 },
            { text: 'University of Tübingen', value: 80 },
            { text: 'Hölderlin tower', value: 60 },
            { text: 'Neckar river', value: 50 },
            { text: 'Top 10 Restaurants in Tübingen', value: 40 },
            { text: 'University Hospital Tübingen', value: 30 },
            { text: 'Epplehaus', value: 20 },
            { text: 'Castle Hohentübingen', value: 20 },
            { text: 'Cyber Valley', value: 20 },
            { text: 'neptune fountain', value: 35},
            { text: 'Botanical garden', value: 20 },
            { text: 'Neckarmüller', value: 20 },

        ];

        wordCloudContainer.innerHTML = '';

        results.forEach((word) => {
            const wordElement = document.createElement('span');
            wordElement.textContent = word.text;
            wordElement.style.fontSize = `${word.value / 2}px`;
            wordElement.classList.add('word');
            wordCloudContainer.appendChild(wordElement);

            // Random delay to each word's appearance
            const randomDelay = Math.random() * 2000;
            setTimeout(() => {
                wordElement.style.opacity = 1;
                wordElement.style.transform = 'scale(1)';
            }, randomDelay);
        });
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
