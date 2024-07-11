document.addEventListener('DOMContentLoaded', () => {
    const searchBar = document.getElementById('searchBar');
    const wordCloudContainer = document.getElementById('wordCloudContainer');

    searchBar.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            const query = searchBar.value.trim();
            if (query) {
                generateWordCloud(query);
                searchBar.classList.add('hidden');
                wordCloudContainer.classList.remove('hidden');
            }
        }
    });
    backButton.addEventListener('click', () => {
        fadeOut(wordCloudContainer, () => {
            wordCloudContainer.classList.add('hidden');
            fadeOut(backButton, () => {
                backButton.classList.add('hidden');
                searchBar.classList.remove('hidden');
                fadeIn(searchBar);
            });
        });
    });


    function generateWordCloud(query) {
        // Simulate search result data
        const results = [
            { text: 'Tübingen Concerts', value: 100 },
            { text: 'University of Tübingen', value: 80 },
            { text: 'Hölderlin tower', value: 60 },
            { text: 'Neckar river', value: 50 },
            { text: 'Top 10 Restaurants in Tübingen', value: 40 },
            { text: 'University Hospital Tübingen', value: 30 },
            { text: 'Cyber Valley', value: 10 }
            // Add more words as needed
        ];

        wordCloudContainer.innerHTML = '';

        results.forEach(word => {
            const wordElement = document.createElement('span');
            wordElement.textContent = word.text;
            wordElement.style.fontSize = `${word.value / 2}px`;
            wordElement.classList.add('word');
            wordCloudContainer.appendChild(wordElement);
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
