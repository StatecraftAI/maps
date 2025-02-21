// Get modal elements
const modal = document.getElementById("share-modal");
const openModalBtn = document.getElementById("spread-the-word-btn");
const closeModalBtn = document.getElementById("close-modal-btn");

// Open the modal when the button is clicked
openModalBtn.onclick = function() {
    modal.style.display = "flex"; // Show the modal
}

// Close the modal when the close button is clicked
closeModalBtn.onclick = function() {
    modal.style.display = "none"; // Hide the modal
}

// Close the modal if the user clicks outside of it
window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }
}
